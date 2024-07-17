//! Mock API implementation directly using the Java Native Interface

use std::ffi::c_void;
use std::sync::OnceLock;

use eyre::Result;
use jni::{
    objects::{GlobalRef, JClass, JMethodID, JPrimitiveArray, JString, JValue, ReleaseMode},
    signature::{Primitive, ReturnType},
    sys::{jboolean, jbyte, jint, jlong},
    InitArgsBuilder, JNIEnv, JavaVM, NativeMethod,
};
use parking_lot::Mutex;
use ticket_sale_core::RequestKind;
use tokio::sync::oneshot;
use tokio::task::{self, JoinHandle};
use uuid::Uuid;

use super::{check_send_result, Api, RequestMsg, Response};

// spell-checker:ignore jboolean,jbyte,jint,jlong,jstring

static JVM: OnceLock<JavaVM> = OnceLock::new();
static JVM_RC: Mutex<u32> = Mutex::new(0);

#[derive(Clone)]
struct JniContext {
    mock_request_cls: GlobalRef,
    mock_request_init: JMethodID,
    request_handler: GlobalRef,
    request_handler_handle: JMethodID,
}

pub struct JniBalancer {
    context: JniContext,
    join_handles: Vec<JoinHandle<()>>,
}

fn init_jvm(class_path: &str, enable_assertions: bool) -> JavaVM {
    let mut builder = InitArgsBuilder::new().version(jni::JNIVersion::V8);
    if enable_assertions {
        builder = builder.option("-ea");
    }
    // spell-checker:disable-next-line
    let builder = builder.option(format!("-Djava.class.path={class_path}"));

    let args = builder.build().expect("Invalid JVM args");
    JavaVM::new(args).expect("Creating the JVM failed")
}

fn channel_to_jni(channel: oneshot::Sender<Response>) -> jlong {
    sptr::Strict::expose_addr(Box::into_raw(Box::new(channel))) as u64 as _
}
/// SAFETY: `ptr` must have been created from `channel_to_jni()`
unsafe fn channel_from_jni(ptr: jlong) -> oneshot::Sender<Response> {
    let ptr = sptr::from_exposed_addr_mut(ptr as u64 as _);
    *unsafe { Box::from_raw(ptr) }
}

pub async fn start(
    threads: u16,
    config: &ticket_sale_core::Config,
    class_path: &str,
    enable_assertions: bool,
) -> Result<(JniBalancer, Api)> {
    let jvm = JVM.get_or_init(|| init_jvm(class_path, enable_assertions));
    *JVM_RC.lock() += 1;

    let mut env = jvm.attach_current_thread()?;

    let mock_request_cls = env.find_class("com/pseuco/cp24/request/MockRequest")?;

    env.register_native_methods(
        &mock_request_cls,
        &[
            NativeMethod {
                name: "respondWithError".into(),
                sig: "(JLjava/lang/String;ZJJJJ)V".into(), // spell-checker:disable-line
                fn_ptr: Java_com_pseuco_cp24_request_MockRequest_respondWithError as *mut c_void,
            },
            NativeMethod {
                name: "respondWithInt".into(),
                sig: "(JIZJJJJ)V".into(), // spell-checker:disable-line
                fn_ptr: Java_com_pseuco_cp24_request_MockRequest_respondWithInt as *mut c_void,
            },
            NativeMethod {
                name: "respondWithSoldOut".into(),
                sig: "(JJJJJ)V".into(), // spell-checker:disable-line
                fn_ptr: Java_com_pseuco_cp24_request_MockRequest_respondWithSoldOut as *mut c_void,
            },
            NativeMethod {
                name: "respondWithServerIds".into(),
                sig: "(J[J)V".into(), // spell-checker:disable-line
                fn_ptr: Java_com_pseuco_cp24_request_MockRequest_respondWithServerIds
                    as *mut c_void,
            },
            NativeMethod {
                name: "writeOutByte".into(),
                sig: "(I)V".into(),
                fn_ptr: Java_com_pseuco_cp24_request_MockRequest_writeOutByte as *mut c_void,
            },
            NativeMethod {
                name: "writeErrByte".into(),
                sig: "(I)V".into(),
                fn_ptr: Java_com_pseuco_cp24_request_MockRequest_writeErrByte as *mut c_void,
            },
            NativeMethod {
                name: "writeOut".into(),
                sig: "([BII)V".into(),
                fn_ptr: Java_com_pseuco_cp24_request_MockRequest_writeOut as *mut c_void,
            },
            NativeMethod {
                name: "writeErr".into(),
                sig: "([BII)V".into(),
                fn_ptr: Java_com_pseuco_cp24_request_MockRequest_writeErr as *mut c_void,
            },
        ],
    )?;
    env.call_static_method(&mock_request_cls, "setupOutput", "()V", &[])
        .unwrap();

    // spell-checker:disable-next-line
    let mock_request_init = env.get_method_id(&mock_request_cls, "<init>", "(JIJJZJJI)V")?;

    assert!(config.tickets <= i32::MAX as u32);
    assert!(config.timeout <= i32::MAX as u32);
    assert!(config.estimator_roundtrip_time <= i32::MAX as u32);
    let j_config = env.new_object(
        "com/pseuco/cp24/Config",
        "(IIIZ)V", // spell-checker:disable-line
        &[
            JValue::Int(config.tickets as jint),
            JValue::Int(config.timeout as jint),
            JValue::Int(config.estimator_roundtrip_time as jint),
            JValue::Bool(config.bonus as jboolean),
        ],
    )?;

    let request_handler = env.call_static_method(
        "com/pseuco/cp24/rocket/Rocket",
        "launch",
        // spell-checker:disable-next-line
        "(Lcom/pseuco/cp24/Config;)Lcom/pseuco/cp24/request/RequestHandler;",
        &[JValue::Object(&j_config)],
    )?;
    let request_handler = env.new_global_ref(request_handler.l()?)?;

    let request_handler_handle = env.get_method_id(
        "com/pseuco/cp24/request/RequestHandler",
        "handle",
        // spell-checker:disable-next-line
        "(Lcom/pseuco/cp24/request/Request;)V",
    )?;

    let mock_request_cls = env.new_global_ref(mock_request_cls)?;
    let context = JniContext {
        mock_request_cls,
        mock_request_init,
        request_handler,
        request_handler_handle,
    };

    let join_handles = (0..threads).map(|_| {
        let (sender, receiver) = flume::bounded::<RequestMsg>(65536);
        let receiver: flume::Receiver<RequestMsg> = receiver.clone();
        let context = context.clone();
        let handle = task::spawn_blocking(move || {
            let mut env = JVM.get().unwrap().attach_current_thread().unwrap();
            for msg in receiver.into_iter() {
                context
                    .make_request(
                        &mut env,
                        msg.kind,
                        msg.payload,
                        msg.customer_id,
                        msg.server_id,
                        msg.response_channel,
                    )
                    .unwrap();
            }
        });
        (sender, handle)
    });
    let (senders, join_handles) = join_handles.unzip();

    let balancer = JniBalancer {
        context,
        join_handles,
    };

    Ok((balancer, Api::new(senders)))
}

impl JniContext {
    fn make_request(
        &self,
        env: &mut JNIEnv,
        kind: RequestKind,
        payload: Option<u32>,
        customer_id: Uuid,
        server_id: Option<Uuid>,
        response_channel: oneshot::Sender<Response>,
    ) -> Result<()> {
        let (cid_m, cid_l) = customer_id.as_u64_pair();
        let (sid_m, sid_l) = server_id.unwrap_or_default().as_u64_pair();
        let payload = match payload {
            Some(i) => {
                debug_assert!(i <= i32::MAX as u32);
                i as i32
            }
            None => -1,
        };

        // long responsePtr
        // int kind
        // long customerL
        // long customerM
        // boolean hasServerId
        // long serverL
        // long serverM
        // int payload
        // SAFETY: the JMethodID is valid and the argument / return types match
        let rq_obj = unsafe {
            env.new_object_unchecked(
                &self.mock_request_cls,
                self.mock_request_init,
                &[
                    JValue::Long(channel_to_jni(response_channel)).as_jni(),
                    JValue::Int(kind as jint).as_jni(),
                    JValue::Long(cid_m as jlong).as_jni(),
                    JValue::Long(cid_l as jlong).as_jni(),
                    JValue::Bool(server_id.is_some() as jboolean).as_jni(),
                    JValue::Long(sid_l as jlong).as_jni(),
                    JValue::Long(sid_m as jlong).as_jni(),
                    JValue::Int(payload).as_jni(),
                ],
            )
        }?;

        // SAFETY: the JMethodID is valid and the argument / return types match
        unsafe {
            env.call_method_unchecked(
                &self.request_handler,
                self.request_handler_handle,
                ReturnType::Primitive(Primitive::Void),
                &[JValue::Object(&rq_obj).as_jni()],
            )
        }?;

        Ok(())
    }
}

impl JniBalancer {
    pub async fn shutdown(self) {
        for handle in self.join_handles {
            handle.await.unwrap();
        }

        let context = self.context;
        let handle = task::spawn_blocking(move || {
            let jvm = JVM.get().unwrap();
            let mut env = jvm.attach_current_thread().unwrap();
            env.call_method(&context.request_handler, "shutdown", "()V", &[])
                .unwrap();

            let mut lock = JVM_RC.lock();
            *lock -= 1;
            if *lock != 0 {
                return;
            }

            let res =
                env.call_static_method(&context.mock_request_cls, "checkShutdown", "()Z", &[]);
            assert!(
                res.unwrap().z().unwrap(),
                "RequestHandler.shutdown() must wait until all other threads have terminated"
            );
        });
        handle.await.unwrap();
    }
}

#[no_mangle]
extern "system" fn Java_com_pseuco_cp24_request_MockRequest_respondWithError<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    response_ptr: jlong,
    msg: JString<'local>,
    has_server_id: jboolean,
    server_lsb: jlong,
    server_msb: jlong,
    customer_lsb: jlong,
    customer_msb: jlong,
) {
    // SAFETY: We recreate the channel passed to `JniContext::make_request()`
    let response_channel = unsafe { channel_from_jni(response_ptr) };
    let server_id = if has_server_id == 0 {
        None
    } else {
        Some(Uuid::from_u64_pair(server_msb as u64, server_lsb as u64))
    };
    let customer_id = Uuid::from_u64_pair(customer_msb as u64, customer_lsb as u64);
    // SAFETY: The given message is a `java.lang.String`
    let msg = match unsafe { env.get_string_unchecked(&msg) } {
        Ok(str) => str.to_string_lossy().into_owned(),
        Err(_) => String::new(),
    };
    let response = Response::Error {
        msg,
        server_id,
        customer_id,
    };
    check_send_result(response_channel.send(response));
}

#[no_mangle]
extern "system" fn Java_com_pseuco_cp24_request_MockRequest_respondWithInt<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    response_ptr: jlong,
    int: jint,
    has_server_id: jboolean,
    server_lsb: jlong,
    server_msb: jlong,
    customer_lsb: jlong,
    customer_msb: jlong,
) {
    // SAFETY: We recreate the channel passed to `JniContext::make_request()`
    let response_channel = unsafe { channel_from_jni(response_ptr) };
    let server_id = if has_server_id == 0 {
        None
    } else {
        Some(Uuid::from_u64_pair(server_msb as u64, server_lsb as u64))
    };
    let customer_id = Uuid::from_u64_pair(customer_msb as u64, customer_lsb as u64);
    debug_assert!(int >= 0);
    let response = Response::Int {
        i: int as u32,
        server_id,
        customer_id,
    };
    check_send_result(response_channel.send(response));
}

#[no_mangle]
extern "system" fn Java_com_pseuco_cp24_request_MockRequest_respondWithSoldOut<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    response_ptr: jlong,
    server_lsb: jlong,
    server_msb: jlong,
    customer_lsb: jlong,
    customer_msb: jlong,
) {
    // SAFETY: We recreate the channel passed to `JniContext::make_request()`
    let response_channel = unsafe { channel_from_jni(response_ptr) };
    let server_id = Uuid::from_u64_pair(server_msb as u64, server_lsb as u64);
    let customer_id = Uuid::from_u64_pair(customer_msb as u64, customer_lsb as u64);
    let response = Response::SoldOut {
        server_id: Some(server_id),
        customer_id,
    };
    check_send_result(response_channel.send(response));
}

#[no_mangle]
extern "system" fn Java_com_pseuco_cp24_request_MockRequest_respondWithServerIds<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    response_ptr: jlong,
    server_ids: JPrimitiveArray<'local, jlong>,
) {
    // SAFETY: We recreate the channel passed to `JniContext::make_request()`
    let response_channel = unsafe { channel_from_jni(response_ptr) };
    let mut ids = Vec::new();

    // SAFETY: `server_ids` is freshly created on the Java side and “moved”
    // here. Moreover, there are no concurrent JNI calls in this thread.
    if let Ok(elements) =
        unsafe { env.get_array_elements_critical(&server_ids, ReleaseMode::NoCopyBack) }
    {
        let mut elements = &*elements;
        debug_assert!(elements.len() % 2 == 0);
        ids.reserve_exact(elements.len() / 2);
        while let [lsb, msb, rest @ ..] = elements {
            ids.push(Uuid::from_u64_pair(*msb as u64, *lsb as u64));
            elements = rest;
        }
    }

    check_send_result(response_channel.send(Response::ServerList(ids)));
}

#[no_mangle]
extern "system" fn Java_com_pseuco_cp24_request_MockRequest_writeOutByte<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    byte: jint,
) {
    print!("{}", byte as u32 as u8 as char);
}

#[no_mangle]
extern "system" fn Java_com_pseuco_cp24_request_MockRequest_writeErrByte<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    byte: jint,
) {
    eprint!("{}", byte as u32 as u8 as char);
}

#[no_mangle]
extern "system" fn Java_com_pseuco_cp24_request_MockRequest_writeOut<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    bytes: JPrimitiveArray<'local, jbyte>,
    offset: jint,
    len: jint,
) {
    let offset = offset as usize;
    let len = len as usize;

    if let Ok(bytes) = unsafe { env.get_array_elements_critical(&bytes, ReleaseMode::NoCopyBack) } {
        if let Some(sub) = bytes.get(offset..offset + len) {
            // SAFETY: `i8` and `u8` have the same data layout
            let sub = unsafe { &*(sub as *const [i8] as *const [u8]) };
            print!("{}", String::from_utf8_lossy(sub));
        }
    }
}

#[no_mangle]
extern "system" fn Java_com_pseuco_cp24_request_MockRequest_writeErr<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    bytes: JPrimitiveArray<'local, jbyte>,
    offset: jint,
    len: jint,
) {
    let offset = offset as usize;
    let len = len as usize;

    if let Ok(bytes) = unsafe { env.get_array_elements_critical(&bytes, ReleaseMode::NoCopyBack) } {
        if let Some(sub) = bytes.get(offset..offset + len) {
            // SAFETY: `i8` and `u8` have the same data layout
            let sub = unsafe { &*(sub as *const [i8] as *const [u8]) };
            eprint!("{}", String::from_utf8_lossy(sub));
        }
    }
}
