// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_pool::{Acquisition, Options, SingleplexPool};
use base_scheduler::Cancellation;
use futures_lite::future::{block_on, poll_once};
use std::{
    io::{Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    pin::pin,
    time::Duration,
};

#[test]
fn two_exchanges_reuse_one_tcp_resource_and_retirement_holds_capacity_through_close() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let peer = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        for expected in [b"one", b"two"] {
            let mut data = [0; 3];
            stream.read_exact(&mut data).unwrap();
            assert_eq!(&data, expected);
            stream.write_all(&data).unwrap();
        }
        assert_eq!(stream.read(&mut [0]).unwrap(), 0);
    });
    let pool = SingleplexPool::new(Options {
        max_resources: 1,
        ..Default::default()
    })
    .unwrap();
    let mut registration_id = None;
    let mut connects = 0;
    for payload in [b"one", b"two"] {
        let mut lease = loop {
            match block_on(pool.acquire_or_reserve(&Cancellation::never())).unwrap() {
                Acquisition::Existing(lease) => break lease,
                Acquisition::Build(build) => {
                    let stream = TcpStream::connect(address).unwrap();
                    stream
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .unwrap();
                    stream
                        .set_write_timeout(Some(Duration::from_secs(5)))
                        .unwrap();
                    build.commit(stream).unwrap();
                    connects += 1;
                }
            }
        };
        if let Some(id) = registration_id {
            assert_eq!(lease.id(), id);
        }
        registration_id = Some(lease.id());
        lease.write_all(payload).unwrap();
        let mut reply = [0; 3];
        lease.read_exact(&mut reply).unwrap();
        assert_eq!(&reply, payload);
        // Both directions of this exchange completed before clean release.
        lease.release();
    }
    assert_eq!(connects, 1);
    assert_eq!(pool.prune(0), 1);
    let retired = pool.try_retirement().unwrap();
    block_on(async {
        let never = Cancellation::never();
        let mut replacement = pin!(pool.acquire_or_reserve(&never));
        assert!(poll_once(replacement.as_mut()).await.is_none());
        assert_eq!(pool.stats().retiring, 1);
        retired.shutdown(Shutdown::Both).unwrap();
        peer.join().unwrap();
        // Socket closure alone does not release accounting before acknowledgment.
        assert!(poll_once(replacement.as_mut()).await.is_none());
        retired.complete();
        assert!(matches!(replacement.await.unwrap(), Acquisition::Build(_)));
    });
    pool.begin_close();
    block_on(pool.wait_closed(&Cancellation::never())).unwrap();
}
