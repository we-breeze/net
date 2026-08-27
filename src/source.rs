use std::{collections::HashSet, net::SocketAddr, sync::Arc, time::Duration};

use parking_lot::Mutex;
use tokio::{net::lookup_host, sync::watch, task::JoinHandle, time::sleep};

use crate::{NetError, Result};

/// A dynamically replaceable set of addresses for one logical node.
pub trait EndpointSource: Send + Sync {
    fn subscribe(&self) -> watch::Receiver<Arc<[SocketAddr]>>;
}

/// An endpoint source that can be updated by discovery adapters such as Vintage.
#[derive(Clone, Debug)]
pub struct EndpointSet {
    sender: watch::Sender<Arc<[SocketAddr]>>,
}

impl EndpointSet {
    pub fn new(endpoints: impl IntoIterator<Item = SocketAddr>) -> Self {
        let (sender, _) = watch::channel(normalize(endpoints));
        Self { sender }
    }

    /// Atomically replaces the current endpoint snapshot.
    ///
    /// Returns `true` when the snapshot changed.
    pub fn replace(&self, endpoints: impl IntoIterator<Item = SocketAddr>) -> bool {
        let endpoints = normalize(endpoints);
        if self.sender.borrow().as_ref() == endpoints.as_ref() {
            return false;
        }
        self.sender.send_replace(endpoints);
        true
    }

    pub fn snapshot(&self) -> Arc<[SocketAddr]> {
        self.sender.borrow().clone()
    }
}

impl EndpointSource for EndpointSet {
    fn subscribe(&self) -> watch::Receiver<Arc<[SocketAddr]>> {
        self.sender.subscribe()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct DnsOptions {
    pub refresh_interval: Duration,
}

impl Default for DnsOptions {
    fn default() -> Self {
        Self {
            refresh_interval: Duration::from_secs(30),
        }
    }
}

/// Resolves one or more `host:port` names and refreshes them in the background.
#[derive(Clone)]
pub struct DnsSource {
    endpoints: EndpointSet,
    _task: Arc<TaskGuard>,
}

impl DnsSource {
    pub async fn new(
        names: impl IntoIterator<Item = impl Into<String>>,
        options: DnsOptions,
    ) -> Result<Self> {
        if options.refresh_interval.is_zero() {
            return Err(NetError::InvalidConfig(
                "DNS refresh interval must be greater than zero".into(),
            ));
        }

        let mut names: Vec<String> = names.into_iter().map(Into::into).collect();
        names.sort();
        names.dedup();
        if names.is_empty() {
            return Err(NetError::NoEndpoints);
        }

        let endpoints = EndpointSet::new(resolve_names(&names).await?);
        let updater = endpoints.clone();
        let handle = tokio::spawn(async move {
            loop {
                sleep(options.refresh_interval).await;
                match resolve_names(&names).await {
                    Ok(next) => {
                        updater.replace(next);
                    }
                    Err(error) => {
                        tracing::warn!(%error, "keeping the previous DNS endpoint snapshot");
                    }
                }
            }
        });

        Ok(Self {
            endpoints,
            _task: Arc::new(TaskGuard(Mutex::new(Some(handle)))),
        })
    }

    pub fn snapshot(&self) -> Arc<[SocketAddr]> {
        self.endpoints.snapshot()
    }
}

impl EndpointSource for DnsSource {
    fn subscribe(&self) -> watch::Receiver<Arc<[SocketAddr]>> {
        self.endpoints.subscribe()
    }
}

struct TaskGuard(Mutex<Option<JoinHandle<()>>>);

impl Drop for TaskGuard {
    fn drop(&mut self) {
        if let Some(handle) = self.0.get_mut().take() {
            handle.abort();
        }
    }
}

async fn resolve_names(names: &[String]) -> Result<Vec<SocketAddr>> {
    let mut endpoints = HashSet::new();
    let mut last_error = None;

    for name in names {
        match lookup_host(name).await {
            Ok(addresses) => endpoints.extend(addresses),
            Err(source) => last_error = Some((name.clone(), source)),
        }
    }

    if endpoints.is_empty() {
        return match last_error {
            Some((name, source)) => Err(NetError::Resolve { name, source }),
            None => Err(NetError::NoEndpoints),
        };
    }

    let mut endpoints: Vec<_> = endpoints.into_iter().collect();
    endpoints.sort_unstable();
    Ok(endpoints)
}

fn normalize(endpoints: impl IntoIterator<Item = SocketAddr>) -> Arc<[SocketAddr]> {
    let mut endpoints: Vec<_> = endpoints.into_iter().collect();
    endpoints.sort_unstable();
    endpoints.dedup();
    endpoints.into()
}
