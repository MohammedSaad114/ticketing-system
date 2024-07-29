//! Mock API implementation directly using the Java Native Interface

use std::ffi::c_void;
use std::mem::ManuallyDrop;
use std::sync::OnceLock;

use eyre::Result;
use jni::{
    objects::{
        GlobalRef, JClass, JObject, JPrimitiveArray, JStaticMethodID, JString, JValue, ReleaseMode,
    },
    signature::{Primitive, ReturnType},
    sys::{jboolean, jbyte, jint, jlong},
    InitArgsBuilder, JNIEnv, JavaVM, NativeMethod,
};
use tokio::sync::oneshot;
use tokio::task;
use uuid::Uuid;

use super::{check_send_result, Api, RequestMsg, Response};

// spell-checker:ignore jboolean,jbyte,jint,jlong,jstring,libtest

// Printing in tests is … complicated. Test cases may be run concurrently, so we
// we want printing output to be associated with the respective test case.
// `libtest` handles this by setting an internal flag which makes the print
// macros capture the output in a thread-local buffer. However, only threads
// spawned from Rust are associated with the respective test case and capture
// printing output accordingly. The solution we use here is as follows: We have
// a Rust thread per `JniBalancer` which reads from a printing stream and
// invokes `print!` or `eprint!`, respectively, for every incoming message. We
// spawn all Java threads (concretely, the one launching the `Rocket` and the
// balancer threads) in a separate `ThreadGroup` per test. When these threads
// spawn new threads, they will be in the same thread group by default. On the
// Java side, we use `System.setOut()` and `System.setErr()` to set the print
// handlers, and they use a `static` `HashMap` to map the current threads’ group
// to the respective print channel print channel.

static JVM: OnceLock<JavaVM> = OnceLock::new();

type PrintSender = flume::Sender<(String, PrintKind)>;

enum PrintKind {
    Regular,
    Error,
    Panic,
}

#[derive(Clone)]
struct BalancerThreadContext {
    receiver: flume::Receiver<RequestMsg>,
    print_sender: PrintSender,
    mock_request_cls: GlobalRef,
    make_request: JStaticMethodID,
}

pub struct JniBalancer {
    rocket_launcher: GlobalRef,
    // In case `shutdown()` is never called, we better leak the sender, because
    // there may still be Java threads running and printing
    print_sender: ManuallyDrop<Box<flume::Sender<(String, PrintKind)>>>,
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
    sptr::Strict::expose_addr(Box::into_raw(Box::new(Some(channel)))) as u64 as _
}
/// SAFETY: `ptr` must have been created from `channel_to_jni()`
unsafe fn channel_from_jni(ptr: jlong) -> oneshot::Sender<Response> {
    let ptr = sptr::from_exposed_addr_mut(ptr as u64 as _);
    Option::take(unsafe { &mut *ptr }).unwrap()
}

#[no_mangle]
unsafe extern "system" fn Java_com_pseuco_cp24_request_MockRequest_dropResponseBox<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    response_ptr: jlong,
) {
    let ptr: *mut Option<oneshot::Sender<Response>> =
        sptr::from_exposed_addr_mut(response_ptr as u64 as _);
    // SAFETY: The caller ensures that the pointer is valid and the box is not dropped twice.
    drop(unsafe { Box::from_raw(ptr) });
}

pub async fn start(
    threads: u16,
    config: &ticket_sale_core::Config,
    class_path: &str,
    enable_assertions: bool,
) -> Result<(JniBalancer, Api)> {
    let mut jvm_init = false;
    let jvm = JVM.get_or_init(|| {
        jvm_init = true;
        init_jvm(class_path, enable_assertions)
    });

    let mut env = jvm.attach_current_thread()?;

    let mock_request_cls = env.find_class("com/pseuco/cp24/request/MockRequest")?;
    let rocket_launcher_cls = env.find_class("com/pseuco/cp24/request/RocketLauncher")?;

    if jvm_init {
        env.register_native_methods(
            &mock_request_cls,
            &[
                NativeMethod {
                    name: "respondWithError".into(),
                    sig: "(JLjava/lang/String;ZJJJJ)V".into(), // spell-checker:disable-line
                    fn_ptr: Java_com_pseuco_cp24_request_MockRequest_respondWithError
                        as *mut c_void,
                },
                NativeMethod {
                    name: "respondWithInt".into(),
                    sig: "(JIZJJJJ)V".into(), // spell-checker:disable-line
                    fn_ptr: Java_com_pseuco_cp24_request_MockRequest_respondWithInt as *mut c_void,
                },
                NativeMethod {
                    name: "respondWithSoldOut".into(),
                    sig: "(JJJJJ)V".into(), // spell-checker:disable-line
                    fn_ptr: Java_com_pseuco_cp24_request_MockRequest_respondWithSoldOut
                        as *mut c_void,
                },
                NativeMethod {
                    name: "respondWithServerIds".into(),
                    sig: "(J[J)V".into(), // spell-checker:disable-line
                    fn_ptr: Java_com_pseuco_cp24_request_MockRequest_respondWithServerIds
                        as *mut c_void,
                },
                NativeMethod {
                    name: "dropResponseBox".into(),
                    sig: "(J)V".into(),
                    fn_ptr: Java_com_pseuco_cp24_request_MockRequest_dropResponseBox as *mut c_void,
                },
            ],
        )?;

        env.register_native_methods(
            &rocket_launcher_cls,
            &[
                NativeMethod {
                    name: "balancerMain".into(),
                    // spell-checker:disable-next-line
                    sig: "(JLcom/pseuco/cp24/request/RequestHandler;)V".into(),
                    fn_ptr: Java_com_pseuco_cp24_request_RocketLauncher_balancerMain as *mut c_void,
                },
                NativeMethod {
                    name: "printByte".into(),
                    sig: "(JZI)V".into(),
                    fn_ptr: Java_com_pseuco_cp24_request_RocketLauncher_printByte as *mut c_void,
                },
                NativeMethod {
                    name: "print".into(),
                    sig: "(JZ[BII)V".into(),
                    fn_ptr: Java_com_pseuco_cp24_request_RocketLauncher_print as *mut c_void,
                },
            ],
        )?;
    }

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

    let make_request = env.get_static_method_id(
        &mock_request_cls,
        "makeRequest",
        // spell-checker:disable-next-line
        "(Lcom/pseuco/cp24/request/RequestHandler;JIJJZJJI)V",
    )?;

    let mock_request_cls = env.new_global_ref(mock_request_cls)?;

    let (print_sender, print_receiver) = flume::unbounded();

    let it = (0..threads).map(|_| {
        let (sender, receiver) = flume::bounded::<RequestMsg>(65536);
        let balancer_context = Box::into_raw(Box::new(BalancerThreadContext {
            receiver,
            print_sender: print_sender.clone(),
            mock_request_cls: mock_request_cls.clone(),
            make_request,
        }));
        let addr = sptr::Strict::expose_addr(balancer_context) as u64 as jlong;
        (sender, addr)
    });
    let (senders, balancer_contexts): (Vec<_>, Vec<_>) = it.unzip();
    let j_balancer_contexts = env.new_long_array(threads as i32)?;
    env.set_long_array_region(&j_balancer_contexts, 0, &balancer_contexts)?;

    std::thread::Builder::new()
        .name("printer".to_string())
        .spawn(move || {
            for (msg, kind) in print_receiver.into_iter() {
                match kind {
                    PrintKind::Regular => print!("{msg}"),
                    PrintKind::Error => eprint!("{msg}"),
                    PrintKind::Panic => panic!("{msg}"),
                }
            }
        })
        .unwrap();
    let print_sender = ManuallyDrop::new(Box::new(print_sender));
    let sender_ptr = &**print_sender as *const flume::Sender<_>;

    let launcher = env.new_object(
        &rocket_launcher_cls,
        "(Lcom/pseuco/cp24/Config;[JJ)V", // spell-checker:disable-line
        &[
            JValue::Object(&j_config),
            JValue::Object(&j_balancer_contexts),
            JValue::Long(sptr::Strict::expose_addr(sender_ptr) as u64 as _),
        ],
    )?;

    let balancer = JniBalancer {
        print_sender,
        rocket_launcher: env.new_global_ref(launcher)?,
    };

    Ok((balancer, Api::new(senders)))
}

impl JniBalancer {
    pub async fn shutdown(self) {
        let launcher = self.rocket_launcher;
        let handle = task::spawn_blocking(move || {
            let jvm = JVM.get().unwrap();
            let mut env = jvm.attach_current_thread().unwrap();
            let res = env.call_method(&launcher, "land", "()Z", &[]).unwrap();
            assert!(
                res.z().unwrap(),
                "RequestHandler.shutdown() must wait until all other threads have terminated"
            );
        });
        handle.await.unwrap();

        drop(ManuallyDrop::into_inner(self.print_sender));
    }
}

#[no_mangle]
unsafe extern "system" fn Java_com_pseuco_cp24_request_MockRequest_respondWithError<'local>(
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
unsafe extern "system" fn Java_com_pseuco_cp24_request_MockRequest_respondWithInt<'local>(
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
unsafe extern "system" fn Java_com_pseuco_cp24_request_MockRequest_respondWithSoldOut<'local>(
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
unsafe extern "system" fn Java_com_pseuco_cp24_request_MockRequest_respondWithServerIds<'local>(
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
unsafe extern "system" fn Java_com_pseuco_cp24_request_RocketLauncher_balancerMain<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    context: jlong,
    request_handler: JObject<'local>,
) {
    // SAFETY: The `Box` was created in `start()`, and the Java side only calls
    // this method once per `context` value.
    let BalancerThreadContext {
        receiver,
        print_sender,
        mock_request_cls,
        make_request,
    } = *unsafe { Box::from_raw(sptr::from_exposed_addr_mut(context as u64 as usize)) };

    for msg in receiver.into_iter() {
        let (cid_m, cid_l) = msg.customer_id.as_u64_pair();
        let (sid_m, sid_l) = msg.server_id.unwrap_or_default().as_u64_pair();
        let payload = match msg.payload {
            Some(i) => {
                debug_assert!(i <= i32::MAX as u32);
                i as i32
            }
            None => -1,
        };

        // RequestHandler balancer
        // long responsePtr
        // int kind
        // long customerL
        // long customerM
        // boolean hasServerId
        // long serverL
        // long serverM
        // int payload
        // SAFETY: the JMethodID is valid and the argument / return types match
        let result = unsafe {
            env.call_static_method_unchecked(
                &mock_request_cls,
                make_request,
                ReturnType::Primitive(Primitive::Void),
                &[
                    JValue::Object(&request_handler).as_jni(),
                    JValue::Long(channel_to_jni(msg.response_channel)).as_jni(),
                    JValue::Int(msg.kind as jint).as_jni(),
                    JValue::Long(cid_m as jlong).as_jni(),
                    JValue::Long(cid_l as jlong).as_jni(),
                    JValue::Bool(msg.server_id.is_some() as jboolean).as_jni(),
                    JValue::Long(sid_l as jlong).as_jni(),
                    JValue::Long(sid_m as jlong).as_jni(),
                    JValue::Int(payload).as_jni(),
                ],
            )
        };
        if let Err(err) = result {
            print_sender.send((err.to_string(), PrintKind::Panic)).ok();
            return;
        }
    }
}

#[no_mangle]
unsafe extern "system" fn Java_com_pseuco_cp24_request_RocketLauncher_printByte<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    stream: jlong,
    is_error: jboolean,
    byte: jint,
) {
    let bytes = &[byte as u32 as u8];
    let msg = String::from_utf8_lossy(bytes);
    if stream == 0 {
        if is_error == 0 {
            print!("{msg}");
        } else {
            eprint!("{msg}");
        }
    } else {
        // SAFETY: The Java side guarantees `stream` to be a valid pointer. (We
        // don’t drop the channel until the shutdown has completed and the
        // address is removed from the `printChannels` map there.)
        let stream: &PrintSender = unsafe { &*sptr::from_exposed_addr(stream as u64 as usize) };
        let kind = if is_error != 0 {
            PrintKind::Error
        } else {
            PrintKind::Regular
        };
        stream.send((msg.into_owned(), kind)).ok();
    }
}

#[no_mangle]
unsafe extern "system" fn Java_com_pseuco_cp24_request_RocketLauncher_print<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    stream: jlong,
    is_error: jboolean,
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
            let msg = String::from_utf8_lossy(sub);
            if stream == 0 {
                if is_error == 0 {
                    print!("{msg}");
                } else {
                    eprint!("{msg}");
                }
            } else {
                // SAFETY: The Java side guarantees `stream` to be a valid pointer. (We
                // don’t drop the channel until the shutdown has completed and the
                // address is removed from the `printChannels` map there.)
                let stream: &PrintSender =
                    unsafe { &*sptr::from_exposed_addr(stream as u64 as usize) };
                let kind = if is_error != 0 {
                    PrintKind::Error
                } else {
                    PrintKind::Regular
                };
                stream.send((msg.into_owned(), kind)).ok();
            }
        }
    }
}
