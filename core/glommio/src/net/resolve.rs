//! Turning an address into socket addresses without stalling the core.
//!
//! `std::net::ToSocketAddrs::to_socket_addrs` on a hostname is `getaddrinfo`,
//! which blocks. Called inline on a thread-per-core runtime it stops *every*
//! task on that executor for the length of the lookup -- single-digit
//! milliseconds warm, seconds when a resolver is unreachable. glommio's own
//! stall detector reports it as a stalled task queue.
//!
//! The lookup therefore belongs on the blocking pool, which is where tokio
//! puts it too. That cannot be done with `std`'s trait: moving the address
//! into another thread needs it `Send + 'static`, and `connect(&host_string)`
//! is not. So the trait here hands back **owned** data first
//! ([`Resolution`]), and only what genuinely needs a resolver crosses to the
//! pool.
//!
//! Everything that can be answered without a resolver is: a `SocketAddr`, a
//! host/port pair whose host is an IP, and a string that parses as an
//! address. Only a real hostname reaches [`spawn_blocking`]. That is also why
//! this bug never showed up in a benchmark -- dialing `127.0.0.1:8080` never
//! touches the resolver.
//!
//! [`spawn_blocking`]: crate::executor::ExecutorProxy::spawn_blocking

use crate::GlommioError;
use std::{
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
};

type Result<T> = crate::Result<T, ()>;

/// What an address turned out to be, before anything blocking has happened.
#[derive(Debug)]
pub enum Resolution {
    /// Already socket addresses. No resolver involved.
    Ready(Vec<SocketAddr>),
    /// A hostname, owned so it can cross to the blocking pool.
    Lookup(String),
}

/// Addresses a glommio socket call accepts.
///
/// Mirrors [`std::net::ToSocketAddrs`] for every shape callers actually pass,
/// and differs in one way that matters: a hostname is resolved on the
/// blocking pool rather than on the executor.
///
/// Sealed. The point is the resolution policy, not extensibility.
pub trait ToSocketAddrs: sealed::Sealed {
    /// Extracts owned addresses, or the hostname that has to be looked up.
    ///
    /// Must not block: anything that would is the caller's business to hand
    /// back as [`Resolution::Lookup`].
    #[doc(hidden)]
    fn resolution(&self) -> io::Result<Resolution>;
}

mod sealed {
    pub trait Sealed {}
}

/// Resolves `addr`, using the blocking pool only if a hostname needs it.
pub(crate) async fn resolve(addr: impl ToSocketAddrs) -> Result<Vec<SocketAddr>> {
    let addresses = match addr.resolution()? {
        Resolution::Ready(addresses) => addresses,
        Resolution::Lookup(host) => {
            crate::executor()
                .spawn_blocking(move || {
                    std::net::ToSocketAddrs::to_socket_addrs(&host[..])
                        .map(|addresses| addresses.collect::<Vec<_>>())
                })
                .await?
        }
    };

    if addresses.is_empty() {
        // Previously an `unwrap` on `None`, which turned a name that resolves
        // to nothing into a panic on a path where every other failure is an
        // error.
        return Err(GlommioError::IoError(io::Error::other(
            "the address resolved to no socket addresses",
        )));
    }
    Ok(addresses)
}

/// The same, for the two `bind` calls that are not `async` and so cannot
/// reach the blocking pool.
///
/// Binding happens once at startup rather than per connection, so resolving
/// inline there costs a stall nobody is waiting through. It still must not
/// panic.
pub(crate) fn resolve_blocking(addr: impl ToSocketAddrs) -> Result<Vec<SocketAddr>> {
    let addresses = match addr.resolution()? {
        Resolution::Ready(addresses) => addresses,
        Resolution::Lookup(host) => std::net::ToSocketAddrs::to_socket_addrs(&host[..])
            .map(|addresses| addresses.collect::<Vec<_>>())?,
    };

    if addresses.is_empty() {
        return Err(GlommioError::IoError(io::Error::other(
            "the address resolved to no socket addresses",
        )));
    }
    Ok(addresses)
}

/// A string is only a lookup if it is not already an address.
fn from_str(text: &str) -> io::Result<Resolution> {
    Ok(match text.parse::<SocketAddr>() {
        Ok(addr) => Resolution::Ready(vec![addr]),
        Err(_) => Resolution::Lookup(text.to_owned()),
    })
}

macro_rules! ready {
    ($($ty:ty => |$value:ident| $body:expr,)*) => {$(
        impl sealed::Sealed for $ty {}
        impl ToSocketAddrs for $ty {
            fn resolution(&self) -> io::Result<Resolution> {
                let $value = self;
                Ok(Resolution::Ready($body))
            }
        }
    )*};
}

ready! {
    SocketAddr => |addr| vec![*addr],
    SocketAddrV4 => |addr| vec![SocketAddr::V4(*addr)],
    SocketAddrV6 => |addr| vec![SocketAddr::V6(*addr)],
    (IpAddr, u16) => |pair| vec![SocketAddr::new(pair.0, pair.1)],
    (Ipv4Addr, u16) => |pair| vec![SocketAddr::V4(SocketAddrV4::new(pair.0, pair.1))],
    (Ipv6Addr, u16) => |pair| vec![SocketAddr::V6(SocketAddrV6::new(pair.0, pair.1, 0, 0))],
    &[SocketAddr] => |addrs| addrs.to_vec(),
    Vec<SocketAddr> => |addrs| addrs.clone(),
}

impl sealed::Sealed for str {}
impl ToSocketAddrs for str {
    fn resolution(&self) -> io::Result<Resolution> {
        from_str(self)
    }
}

impl sealed::Sealed for String {}
impl ToSocketAddrs for String {
    fn resolution(&self) -> io::Result<Resolution> {
        from_str(self)
    }
}

impl sealed::Sealed for (&str, u16) {}
impl ToSocketAddrs for (&str, u16) {
    fn resolution(&self) -> io::Result<Resolution> {
        let (host, port) = *self;
        Ok(match host.parse::<IpAddr>() {
            Ok(ip) => Resolution::Ready(vec![SocketAddr::new(ip, port)]),
            Err(_) => Resolution::Lookup(format!("{host}:{port}")),
        })
    }
}

impl sealed::Sealed for (String, u16) {}
impl ToSocketAddrs for (String, u16) {
    fn resolution(&self) -> io::Result<Resolution> {
        (self.0.as_str(), self.1).resolution()
    }
}

impl<T: ToSocketAddrs + ?Sized> sealed::Sealed for &T {}
impl<T: ToSocketAddrs + ?Sized> ToSocketAddrs for &T {
    fn resolution(&self) -> io::Result<Resolution> {
        (**self).resolution()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{enclose, LocalExecutor};
    use std::{cell::RefCell, rc::Rc};

    #[test]
    fn a_lookup_yields_to_the_executor() {
        // The bug: getaddrinfo called inline stops every task on the core.
        //
        // This has to be asserted here rather than around `TcpStream::connect`,
        // because connect suspends afterwards anyway -- so a neighbouring task
        // runs either way and a test up there passes whether resolution blocks
        // or not. `resolve` has exactly one possible suspension point, the
        // lookup, so a neighbour that runs proves the lookup yielded.
        LocalExecutor::default().run(async {
            let ran = Rc::new(RefCell::new(0usize));
            // The neighbour only counts once the lookup is under way. Without
            // this it counts the poll it gets when it is spawned, which
            // happens whether or not the lookup yields -- and then the test
            // passes either way, which it did.
            let looking_up = Rc::new(RefCell::new(false));

            let neighbour = crate::spawn_local(enclose! { (ran, looking_up) async move {
                loop {
                    if *looking_up.borrow() {
                        *ran.borrow_mut() += 1;
                    }
                    crate::executor().yield_task_queue_now().await;
                }
            }})
            .detach();

            // Let it reach its first suspension, so what follows measures the
            // lookup rather than the spawn.
            crate::executor().yield_task_queue_now().await;
            *looking_up.borrow_mut() = true;

            // In /etc/hosts, so this needs no network and still has to cross
            // to the blocking pool.
            let addresses = resolve("localhost:80").await.unwrap();
            assert!(!addresses.is_empty());

            assert!(
                *ran.borrow() > 0,
                "nothing else ran while a hostname was resolved: the lookup \
                 happened inline and stalled the whole core"
            );

            drop(neighbour);
        });
    }

    #[test]
    fn an_address_resolving_to_nothing_is_an_error() {
        // `.next().unwrap()` used to make this a panic, on a path where every
        // other failure is a Result.
        LocalExecutor::default().run(async {
            let empty: &[SocketAddr] = &[];
            assert!(resolve(empty).await.is_err());
            assert!(resolve_blocking(empty).is_err());
        });
    }

    #[test]
    fn an_address_needs_no_resolver() {
        // The reason this bug never showed up in a benchmark: everything a
        // test dials answers without a lookup.
        for text in ["127.0.0.1:8080", "[::1]:8080"] {
            assert!(
                matches!(text.resolution().unwrap(), Resolution::Ready(_)),
                "{text} should not need a resolver"
            );
        }
        assert!(matches!(
            ("127.0.0.1", 80).resolution().unwrap(),
            Resolution::Ready(_)
        ));
        assert!(matches!(
            SocketAddr::from(([127, 0, 0, 1], 80)).resolution().unwrap(),
            Resolution::Ready(_)
        ));
    }

    #[test]
    fn a_hostname_is_handed_over_for_lookup() {
        assert!(matches!(
            "localhost:80".resolution().unwrap(),
            Resolution::Lookup(host) if host == "localhost:80"
        ));
        assert!(matches!(
            ("localhost", 80).resolution().unwrap(),
            Resolution::Lookup(host) if host == "localhost:80"
        ));
    }
}
