use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc, OnceLock, Weak,
        mpsc::{self, Receiver, RecvTimeoutError, Sender},
    },
    thread,
    time::{Duration, Instant},
};

use crate::{EndpointSet, NetError, Result, node::NodePoolCore};

const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(1);
const MAX_BACKGROUND_CONNECTS: usize = 1;

pub(crate) enum Command {
    Register(MaintenanceNode),
    Finished { id: u64, retry_now: bool },
}

pub(crate) struct MaintenanceNode {
    core: Weak<NodePoolCore>,
    endpoints: EndpointSet,
}

#[derive(Clone)]
struct Maintainer {
    sender: Sender<Command>,
}

static MAINTAINER: OnceLock<std::result::Result<Maintainer, String>> = OnceLock::new();

pub(crate) fn register(core: &Arc<NodePoolCore>, endpoints: EndpointSet) -> Result<()> {
    let maintainer = shared()?;
    maintainer
        .sender
        .send(Command::Register(MaintenanceNode {
            core: Arc::downgrade(core),
            endpoints,
        }))
        .map_err(|_| NetError::PoolMaintainerStopped)
}

fn shared() -> Result<Maintainer> {
    match MAINTAINER.get_or_init(start) {
        Ok(maintainer) => Ok(maintainer.clone()),
        Err(message) => Err(NetError::InvalidConfig(message.clone())),
    }
}

fn start() -> std::result::Result<Maintainer, String> {
    let (sender, receiver) = mpsc::channel();
    let thread_sender = sender.clone();
    thread::Builder::new()
        .name("brz-net-maintenance".into())
        .spawn(move || run(receiver, thread_sender))
        .map_err(|error| format!("failed to start shared node-pool maintainer: {error}"))?;
    Ok(Maintainer { sender })
}

fn run(receiver: Receiver<Command>, sender: Sender<Command>) {
    let mut nodes = HashMap::<u64, MaintenanceNode>::new();
    let mut ready = VecDeque::new();
    let mut queued = HashSet::new();
    let mut in_flight = 0_usize;
    let mut next_tick = Instant::now() + MAINTENANCE_INTERVAL;

    loop {
        let wait = next_tick.saturating_duration_since(Instant::now());
        match receiver.recv_timeout(wait) {
            Ok(command) => {
                handle_command(command, &mut nodes, &mut ready, &mut queued, &mut in_flight)
            }
            Err(RecvTimeoutError::Timeout) => {
                scan_nodes(&mut nodes, &mut ready, &mut queued);
                next_tick = Instant::now() + MAINTENANCE_INTERVAL;
            }
            Err(RecvTimeoutError::Disconnected) => return,
        }

        dispatch(&nodes, &mut ready, &mut queued, &mut in_flight, &sender);
    }
}

fn handle_command(
    command: Command,
    nodes: &mut HashMap<u64, MaintenanceNode>,
    ready: &mut VecDeque<u64>,
    queued: &mut HashSet<u64>,
    in_flight: &mut usize,
) {
    match command {
        Command::Register(node) => {
            let Some(core) = node.core.upgrade() else {
                return;
            };
            let id = core.maintenance_id();
            if core.maintenance_tick(&node.endpoints) {
                enqueue(id, ready, queued);
            }
            nodes.insert(id, node);
        }
        Command::Finished { id, retry_now } => {
            *in_flight = in_flight.saturating_sub(1);
            if retry_now {
                enqueue(id, ready, queued);
            }
        }
    }
}

fn scan_nodes(
    nodes: &mut HashMap<u64, MaintenanceNode>,
    ready: &mut VecDeque<u64>,
    queued: &mut HashSet<u64>,
) {
    nodes.retain(|id, node| {
        let Some(core) = node.core.upgrade() else {
            queued.remove(id);
            return false;
        };
        if core.maintenance_tick(&node.endpoints) {
            enqueue(*id, ready, queued);
        }
        true
    });
}

fn enqueue(id: u64, ready: &mut VecDeque<u64>, queued: &mut HashSet<u64>) {
    if queued.insert(id) {
        ready.push_back(id);
    }
}

fn dispatch(
    nodes: &HashMap<u64, MaintenanceNode>,
    ready: &mut VecDeque<u64>,
    queued: &mut HashSet<u64>,
    in_flight: &mut usize,
    sender: &Sender<Command>,
) {
    while *in_flight < MAX_BACKGROUND_CONNECTS {
        let Some(id) = ready.pop_front() else {
            return;
        };
        queued.remove(&id);

        let Some(node) = nodes.get(&id) else {
            continue;
        };
        let Some(core) = node.core.upgrade() else {
            continue;
        };
        let Some((endpoints, reservation)) =
            core.reserve_background_connection(node.endpoints.clone(), sender.clone())
        else {
            continue;
        };

        *in_flight += 1;
        core.runtime().spawn(reservation.connect(endpoints));
    }
}
