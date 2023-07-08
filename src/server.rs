use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};

use tonic::{transport::Server, Request, Response, Status};

use cluster::cluster_server::{Cluster, ClusterServer};
use cluster::{ClusterHost, ClusterHostId, ClusterId, ClusterStatus, Host, HostId, Hosts, Info};
use rand::Rng;

pub mod cluster {
    tonic::include_proto!("cluster");
}

const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz-";

fn random_string() -> String {
    let mut rng = rand::thread_rng();

    (0..30)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

const DEFAULT_TIMEOUT_MILLISECONDS: u64 = 200;

type Clusters = HashMap<String, HashMap<String, FakeHost>>;
type Ip = String;
type Port = u32;

#[derive(Debug, PartialEq, Eq)]
struct FakeHost {
    name: String,
    is_up: bool,
    ip: Ip,
    port: Port,
    timeout_milliseconds: u64,
    friends: HashSet<(Ip, Port)>,
}

impl Default for FakeHost {
    fn default() -> Self {
        FakeHost {
            name: random_string(),
            is_up: true,
            ip: random_string(),
            port: 1,
            timeout_milliseconds: DEFAULT_TIMEOUT_MILLISECONDS,
            friends: HashSet::new(),
        }
    }
}

#[derive(Debug)]
struct MemoryBackend {
    clusters: Arc<Mutex<Clusters>>,
}

struct ClusterHostIdStruct {
    cluster_name: String,
    host_name: String,
}

impl TryFrom<Request<ClusterHostId>> for ClusterHostIdStruct {
    type Error = Status;
    fn try_from(request: Request<ClusterHostId>) -> Result<Self, Self::Error> {
        request.into_inner().try_into()
    }
}

impl TryFrom<ClusterHostId> for ClusterHostIdStruct {
    type Error = Status;
    fn try_from(cluster_host: ClusterHostId) -> Result<Self, Self::Error> {
        Ok(ClusterHostIdStruct {
            cluster_name: cluster_host
                .cluster
                .ok_or_else(|| Status::invalid_argument("cluster must be present."))?
                .name,
            host_name: cluster_host
                .host
                .ok_or_else(|| Status::invalid_argument("host must be present."))?
                .name,
        })
    }
}

impl TryFrom<&ClusterHost> for ClusterHostIdStruct {
    type Error = Status;
    fn try_from(cluster_host: &ClusterHost) -> Result<Self, Self::Error> {
        Ok(ClusterHostIdStruct {
            cluster_name: cluster_host
                .cluster
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("cluster must be present."))?
                .name
                .clone(),
            host_name: cluster_host
                .host
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("host must be present."))?
                .id
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("host's id must be present."))?
                .name
                .clone(),
        })
    }
}

impl MemoryBackend {
    fn lock_clusters(&self) -> Result<MutexGuard<'_, Clusters>, Status> {
        self.clusters.as_ref().lock().map_err(|_e| {
            Status::internal("Some other thread panicked while holding the mutex to the data")
        })
    }

    fn get_mut_cluster<'c>(
        &self,
        cluster_name: &String,
        clusters: &'c mut MutexGuard<'_, HashMap<String, HashMap<String, FakeHost>>>,
    ) -> Result<&'c mut HashMap<String, FakeHost>, Status> {
        clusters.get_mut(cluster_name).ok_or_else(|| {
            Status::not_found(format!("{:?} is not a known cluster name", cluster_name))
        })
    }

    fn get_mut_host<'c>(
        &self,
        cluster_host: &ClusterHostIdStruct,
        clusters: &'c mut MutexGuard<'_, HashMap<String, HashMap<String, FakeHost>>>,
    ) -> Result<&'c mut FakeHost, Status> {
        let cluster_name = &cluster_host.cluster_name;
        let host_name = &cluster_host.host_name;
        self.get_mut_cluster(cluster_name, clusters)?
            .get_mut(host_name)
            .ok_or_else(|| {
                Status::not_found(format!(
                    "{:?} is not a known host name for the cluster {:?}",
                    host_name, cluster_name
                ))
            })
    }
}

impl Info {
    fn new(cluster: &HashMap<String, FakeHost>, cluster_name: String) -> Info {
        let (hosts, online_hosts) = cluster
            .values()
            .fold((0, 0), |(hosts, online_hosts), host| {
                (
                    hosts + 1,
                    if host.is_up {
                        online_hosts + 1
                    } else {
                        online_hosts
                    },
                )
            });
        Info {
            id: Some(ClusterId { name: cluster_name }),
            hosts,
            online_hosts,
            status: if hosts == online_hosts {
                // This is "as" it should be (https://github.com/tokio-rs/prost#enumerations)
                ClusterStatus::Ok as i32
            } else if hosts >= online_hosts / 2 {
                ClusterStatus::Warn as i32
            } else {
                ClusterStatus::Fail as i32
            },
        }
    }
}

impl From<&FakeHost> for Host {
    fn from(host: &FakeHost) -> Self {
        Host {
            id: Some(HostId {
                name: host.name.clone(),
            }),
            is_up: host.is_up,
            ip: host.ip.clone(),
            port: host.port,
            timeout_milliseconds: host.timeout_milliseconds,
        }
    }
}

impl From<&mut FakeHost> for Host {
    fn from(host: &mut FakeHost) -> Self {
        (&*host).into()
    }
}

#[tonic::async_trait]
impl Cluster for MemoryBackend {
    async fn info(&self, request: Request<ClusterId>) -> Result<Response<Info>, Status> {
        let cluster_name = request.into_inner().name;

        Ok(Response::new(Info::new(
            self.get_mut_cluster(&cluster_name, &mut self.lock_clusters()?)?,
            cluster_name,
        )))
    }

    async fn new_cluster(&self, request: Request<ClusterId>) -> Result<Response<Info>, Status> {
        let name = request.into_inner().name;
        let mut clusters = self.lock_clusters()?;

        if clusters.contains_key(&name) {
            return Err(Status::already_exists(format!(
                "A cluster with the following name already exists: {:?}",
                name
            )));
        };
        let mut first_host = FakeHost::default();
        let mut second_host = FakeHost::default();
        first_host
            .friends
            .insert((second_host.ip.clone(), second_host.port));
        second_host
            .friends
            .insert((first_host.ip.clone(), first_host.port));
        let cluster = HashMap::from([
            (first_host.name.clone(), first_host),
            (second_host.name.clone(), second_host),
        ]);
        let info = Info::new(&cluster, name.clone());
        clusters.insert(name, cluster);
        Ok(Response::new(info))
    }

    async fn destroy_cluster(&self, request: Request<ClusterId>) -> Result<Response<Info>, Status> {
        let cluster_name = request.into_inner().name;

        Ok(Response::new(Info::new(
            &self.lock_clusters()?.remove(&cluster_name).ok_or_else(|| {
                Status::not_found(format!("{:?} is not a known cluster name", cluster_name))
            })?,
            cluster_name,
        )))
    }

    async fn hosts(&self, request: Request<ClusterId>) -> Result<Response<Hosts>, Status> {
        let cluster_name = request.into_inner().name;
        Ok(Response::new(Hosts {
            hosts: self
                .get_mut_cluster(&cluster_name, &mut self.lock_clusters()?)?
                .values()
                .map(|h| h.into())
                .collect(),
        }))
    }

    async fn add_host(&self, request: Request<ClusterId>) -> Result<Response<Host>, Status> {
        let cluster_name = request.into_inner().name;
        let mut clusters = self.lock_clusters()?;
        let cluster = self.get_mut_cluster(&cluster_name, &mut clusters)?;
        if cluster.len() >= 100 {
            return Err(Status::failed_precondition(
                format!("Adding a host to the cluster {:?} would make it have more than the maximum number of 100 hosts.", cluster_name)
            ));
        }
        let mut host = FakeHost::default();
        for other_host in cluster.values_mut() {
            other_host.friends.insert((host.ip.clone(), host.port));
            host.friends
                .insert((other_host.ip.clone(), other_host.port));
        }
        let name = host.name.clone();
        // EXPECT: Because a host gets assigned a random name, we assume it won't collide with another host's name
        cluster
            .insert(host.name.clone(), host)
            .ok_or(())
            .expect_err("Host name collision");
        // UNWRAP: we just added this host 🙄
        Ok(Response::new(cluster.get(&name).unwrap().into()))
    }

    async fn destroy_host(
        &self,
        request: Request<ClusterHostId>,
    ) -> Result<Response<Host>, Status> {
        let ClusterHostIdStruct {
            cluster_name,
            host_name,
        } = ClusterHostIdStruct::try_from(request.into_inner())?;
        let mut clusters = self.lock_clusters()?;
        let cluster = self.get_mut_cluster(&cluster_name, &mut clusters)?;
        if cluster.len() <= 2 {
            Err(Status::failed_precondition(
                format!("Removing a host from the cluster {:?} would make it have less than the minimum number of 2 hosts.", cluster_name)
            ))
        } else {
            Ok(Response::new(
                (&cluster.remove(&host_name).ok_or_else(|| {
                    Status::not_found(format!(
                        "{:?} is not a host known to the cluster {:?}",
                        host_name, cluster_name
                    ))
                })?)
                    .into(),
            ))
        }
    }

    async fn patch_host(&self, request: Request<ClusterHost>) -> Result<Response<Host>, Status> {
        let mut clusters = self.lock_clusters()?;
        let request_inner = request.into_inner();
        let cluster_host = ClusterHostIdStruct::try_from(&request_inner)?;
        let Host {
            id,
            is_up: _,
            ip,
            port,
            timeout_milliseconds,
        } = request_inner.host.unwrap();

        let old_ip_port;
        let new_ip_port;
        let response;
        {
            // Update the host
            let mut mut_host = self.get_mut_host(&cluster_host, &mut clusters)?;
            old_ip_port = (mut_host.ip.clone(), mut_host.port);
            // UNWRAP: if the above `.try_into()` succeeded, then the host must exist
            if let Some(HostId { name }) = id {
                if name.len() > 0 {
                    mut_host.name = name.clone();
                }
            }
            if ip.len() > 0 {
                mut_host.ip = ip;
            }
            if port > 0 {
                mut_host.port = port;
            }
            if timeout_milliseconds > 0 {
                mut_host.timeout_milliseconds = timeout_milliseconds;
            }
            response = Response::new(mut_host.into());
            new_ip_port = (mut_host.ip.clone(), mut_host.port);
        }
        // Inform the other hosts about the new address of this host.
        if port > 0 || timeout_milliseconds > 0 {
            // UNWRAP: if the above `.try_into()` succeeded, then the cluster must exist
            let hosts = clusters.get_mut(&cluster_host.cluster_name).unwrap();
            for some_host in hosts.values_mut() {
                // Don't add a hint to the host that we just changed
                if some_host.friends.remove(&old_ip_port) {
                    some_host.friends.insert(new_ip_port.clone());
                }
            }
        }
        Ok(response)
    }

    async fn take_down(&self, request: Request<ClusterHostId>) -> Result<Response<Host>, Status> {
        let mut clusters = self.lock_clusters()?;
        let mut host = self.get_mut_host(&request.try_into()?, &mut clusters)?;
        if !host.is_up {
            Err(Status::failed_precondition(
                "The specified host is already down.",
            ))
        } else {
            host.is_up = false;
            Ok(Response::new(host.into()))
        }
    }

    async fn bring_up(&self, request: Request<ClusterHostId>) -> Result<Response<Host>, Status> {
        let mut clusters = self.lock_clusters()?;
        let mut host = self.get_mut_host(&request.try_into()?, &mut clusters)?;
        if host.is_up {
            Err(Status::failed_precondition(
                "The specified host is already up.",
            ))
        } else {
            host.is_up = true;
            Ok(Response::new(host.into()))
        }
    }
}

impl Default for MemoryBackend {
    fn default() -> Self {
        return MemoryBackend {
            clusters: Arc::new(Mutex::new(HashMap::new())),
        };
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let backend = MemoryBackend::default();

    Server::builder()
        .add_service(ClusterServer::new(backend))
        .serve(addr)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_test_backend() -> MemoryBackend {
        MemoryBackend {
            clusters: Arc::new(Mutex::new(HashMap::from([(
                "cluster".to_string(),
                HashMap::from([
                    (
                        "ho".to_owned(),
                        FakeHost {
                            name: "ho".to_string(),
                            is_up: true,
                            ip: "fakeip".to_string(),
                            port: 1,
                            timeout_milliseconds: DEFAULT_TIMEOUT_MILLISECONDS,
                            friends: HashSet::from([("fakeotherip".to_string(), 1)]),
                        },
                    ),
                    (
                        "st".to_owned(),
                        FakeHost {
                            name: "st".to_string(),
                            is_up: false,
                            ip: "fakeotherip".to_string(),
                            port: 1,
                            timeout_milliseconds: DEFAULT_TIMEOUT_MILLISECONDS,
                            friends: HashSet::from([("fakeip".to_string(), 1)]),
                        },
                    ),
                ]),
            )]))),
        }
    }

    #[tokio::test]
    async fn test_info_not_found() {
        let backend = MemoryBackend::default();

        let status = backend
            .info(Request::new(ClusterId {
                name: "doesn't exist".to_string(),
            }))
            .await
            .unwrap_err();

        assert_eq!(status.code(), tonic::Code::NotFound);
        assert_eq!(
            status.message(),
            "\"doesn't exist\" is not a known cluster name"
        );
    }

    #[tokio::test]
    async fn test_info_minimal() {
        let backend = get_test_backend();

        let info = backend
            .info(Request::new(ClusterId {
                name: "cluster".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            info,
            Info {
                id: Some(ClusterId {
                    name: "cluster".to_string()
                }),
                status: ClusterStatus::Warn as i32,
                hosts: 2,
                online_hosts: 1
            }
        );
    }

    #[tokio::test]
    async fn test_new_cluster() {
        let backend = MemoryBackend::default();
        let name = "name".to_string();

        let info = backend
            .new_cluster(Request::new(ClusterId { name: name.clone() }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            info,
            Info {
                id: Some(ClusterId { name }),
                status: ClusterStatus::Ok as i32,
                hosts: 2,
                online_hosts: 2
            }
        );
    }

    #[tokio::test]
    async fn test_add_host() {
        let backend = get_test_backend();

        backend
            .add_host(Request::new(ClusterId {
                name: "cluster".to_string(),
            }))
            .await
            .unwrap();

        assert_eq!(
            Arc::into_inner(backend.clusters)
                .unwrap()
                .into_inner()
                .unwrap()
                .into_values()
                .map(|c| c.into_values().len())
                .sum::<usize>(),
            3
        );
    }

    #[tokio::test]
    async fn test_destroy_host() {
        let backend = get_test_backend();

        backend
            .add_host(Request::new(ClusterId {
                name: "cluster".to_string(),
            }))
            .await
            .unwrap();

        let host = backend
            .destroy_host(Request::new(ClusterHostId {
                cluster: Some(ClusterId {
                    name: "cluster".to_string(),
                }),
                host: Some(HostId {
                    name: "ho".to_string(),
                }),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            host,
            Host {
                id: Some(HostId {
                    name: "ho".to_string()
                }),
                is_up: true,
                ip: "fakeip".to_string(),
                port: 1,
                timeout_milliseconds: DEFAULT_TIMEOUT_MILLISECONDS,
            },
        );
    }
}
