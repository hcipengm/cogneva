//! 单测共用的样例：一个没人占着的端口。
//!
//! 它不是任何一处的题目，是每处要起一个真 socket 的单测都要先拿到的东西。原先
//! gRPC 生命周期与插件两处各写了一遍，取端口的方式（先 bind 让内核挑、读回来、
//! 再放手）一变，两份不会同时红——而这种方式本身有代价，两份都写着才看得出。

/// 一个没人占着的端口：先绑上让内核挑，读回来，再放手。
///
/// 放手到服务端自己绑上之间有一个窗口，这是「在真 socket 上测」的代价，也是为什么
/// 端口是这么取的而不是写死一个数。
pub(crate) fn free_port() -> std::net::SocketAddr {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("the OS hands out a port");
    let addr = probe.local_addr().expect("a bound listener has an address");
    drop(probe);
    addr
}
