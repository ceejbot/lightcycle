//! HTTP Load Balancer example using consistent hashing for sticky sessions
//!
//! This example demonstrates how to use ConsistentRing to implement a load balancer
//! that maintains session affinity while distributing load across backend servers.
//!
//! Run with: cargo run --example load_balancer

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use lightcycle::{ConsistentRing, HasId, HashRing};

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct BackendServer {
    id: String,
    host: String,
    port: u16,
    healthy: Arc<Mutex<bool>>,
    last_health_check: Arc<Mutex<Instant>>,
    active_connections: Arc<Mutex<usize>>,
    total_requests: Arc<Mutex<u64>>,
    response_times: Arc<Mutex<Vec<Duration>>>,
    cpu_usage: Arc<Mutex<f32>>,
}

impl BackendServer {
    fn new(host: impl Into<String>, port: u16) -> Self {
        let host = host.into();
        let id = format!("{}:{}", host, port);

        Self {
            id: id.clone(),
            host,
            port,
            healthy: Arc::new(Mutex::new(true)),
            last_health_check: Arc::new(Mutex::new(Instant::now())),
            active_connections: Arc::new(Mutex::new(0)),
            total_requests: Arc::new(Mutex::new(0)),
            response_times: Arc::new(Mutex::new(Vec::new())),
            cpu_usage: Arc::new(Mutex::new(0.0)),
        }
    }

    fn health_check(&self) -> bool {
        let mut last_check = self.last_health_check.lock().expect("poisoned mutex");

        if last_check.elapsed() > Duration::from_secs(10) {
            *last_check = Instant::now();

            // Simulate health check
            let connections = *self.active_connections.lock().expect("poisoned mutex");
            let cpu = *self.cpu_usage.lock().expect("poisoned mutex");

            // Consider unhealthy if too many connections or high CPU
            let is_healthy = connections < 100 && cpu < 0.9;
            *self.healthy.lock().expect("poisoned mutex") = is_healthy;

            if !is_healthy {
                println!(
                    "⚠️  {} is unhealthy (connections: {}, CPU: {:.1}%)",
                    self.id,
                    connections,
                    cpu * 100.0
                );
            }
        }

        *self.healthy.lock().expect("poisoned mutex")
    }

    fn handle_request(&self, request: &HttpRequest) -> HttpResponse {
        if !self.health_check() {
            return HttpResponse::error(503, "Service Unavailable");
        }

        let mut connections = self.active_connections.lock().expect("poisoned mutex");
        *connections += 1;

        let mut total = self.total_requests.lock().expect("poisoned mutex");
        *total += 1;

        // Simulate request processing
        let start = Instant::now();

        // Simulate varying response times
        let _processing_time = Duration::from_millis((random_float() * 100.0) as u64 + 10);

        std::thread::sleep(Duration::from_millis(1)); // Simulate some work

        let elapsed = start.elapsed();
        self.response_times.lock().expect("poisoned mutex").push(elapsed);

        // Update CPU usage simulation
        *self.cpu_usage.lock().expect("poisoned mutex") = random_float() * 0.5 + (*connections as f32 / 200.0);

        *connections -= 1;

        println!("  → {} handled {} ({}ms)", self.id, request.path, elapsed.as_millis());

        HttpResponse::ok(format!("Response from {}", self.id))
    }

    fn stats(&self) -> ServerStats {
        let response_times = self.response_times.lock().expect("poisoned mutex");
        let avg_response_time = if !response_times.is_empty() {
            let sum: Duration = response_times.iter().sum();
            sum / response_times.len() as u32
        } else {
            Duration::ZERO
        };

        ServerStats {
            id: self.id.clone(),
            healthy: *self.healthy.lock().expect("poisoned mutex"),
            active_connections: *self.active_connections.lock().expect("poisoned mutex"),
            total_requests: *self.total_requests.lock().expect("poisoned mutex"),
            avg_response_time,
            cpu_usage: *self.cpu_usage.lock().expect("poisoned mutex"),
        }
    }
}

impl HasId for BackendServer {
    fn id(&self) -> &str {
        &self.id
    }
}

#[derive(Debug)]
struct HttpRequest {
    path: String,
    session_id: Option<String>,
    client_ip: String,
}

#[derive(Debug)]
struct HttpResponse {
    status: u16,
    body: String,
}

impl HttpResponse {
    fn ok(body: impl Into<String>) -> Self {
        Self {
            status: 200,
            body: body.into(),
        }
    }

    fn error(status: u16, body: impl Into<String>) -> Self {
        Self {
            status,
            body: body.into(),
        }
    }
}

#[derive(Debug)]
struct ServerStats {
    id: String,
    healthy: bool,
    active_connections: usize,
    total_requests: u64,
    avg_response_time: Duration,
    cpu_usage: f32,
}

struct LoadBalancer {
    ring: Arc<Mutex<ConsistentRing>>,
    servers: Vec<BackendServer>,
    session_affinity: Arc<Mutex<HashMap<String, String>>>,
}

impl LoadBalancer {
    fn new(servers: Vec<BackendServer>) -> Self {
        let mut ring = ConsistentRing::new_with_replica_count(100);

        for server in &servers {
            ring.add(Box::new(server.clone()));
        }

        Self {
            ring: Arc::new(Mutex::new(ring)),
            servers,
            session_affinity: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn route_request(&self, request: &HttpRequest) -> HttpResponse {
        let ring = self.ring.lock().expect("poisoned mutex");

        // Use session ID for sticky sessions, fall back to client IP
        let routing_key = request.session_id.as_ref().unwrap_or(&request.client_ip);

        // Check session affinity cache
        let mut affinity = self.session_affinity.lock().expect("poisoned mutex");

        if let Some(server_id) = affinity.get(routing_key) {
            // Try to use the affinity server if it's still healthy
            for server in &self.servers {
                if server.id() == server_id && server.health_check() {
                    println!("↻ Session affinity: {} -> {}", routing_key, server_id);
                    return server.handle_request(request);
                }
            }
        }

        // Use consistent hashing to find a server
        if let Some(server_id_box) = ring.locate(routing_key) {
            let server_id = server_id_box.id();
            // Find the actual server instance
            for server in &self.servers {
                if server.id() == server_id {
                    // Update session affinity
                    affinity.insert(routing_key.to_string(), server_id.to_string());
                    return server.handle_request(request);
                }
            }
        }

        // All servers down
        HttpResponse::error(503, "No healthy servers available")
    }

    fn rebalance(&mut self) {
        println!("\n=== Rebalancing servers ===");
        let mut ring = self.ring.lock().expect("poisoned mutex");

        for server in &self.servers {
            if !server.health_check() {
                println!("Removing unhealthy server: {}", server.id);
                ring.remove(server.id()).ok();

                // Clear session affinity for this server
                let mut affinity = self.session_affinity.lock().expect("poisoned mutex");
                affinity.retain(|_, v| v != server.id());
            }
        }

        for server in &self.servers {
            if server.health_check() && ring.locate(server.id()).is_none() {
                println!("Re-adding healthy server: {}", server.id);
                ring.add(Box::new(server.clone()));
            }
        }
    }

    fn stats(&self) -> Vec<ServerStats> {
        self.servers.iter().map(|s| s.stats()).collect()
    }
}

fn random_float() -> f32 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("should get system time")
        .subsec_nanos();
    (nanos % 1000) as f32 / 1000.0
}

fn generate_client_ip() -> String {
    format!("192.168.1.{}", (random_float() * 255.0) as u8)
}

fn generate_session_id() -> Option<String> {
    // 70% of requests have a session
    if random_float() < 0.7 {
        Some(format!("session_{}", (random_float() * 1000.0) as u32))
    } else {
        None
    }
}

fn main() {
    println!("=== HTTP Load Balancer with Sticky Sessions ===\n");

    // Create backend servers
    let servers = vec![
        BackendServer::new("backend1.local", 8001),
        BackendServer::new("backend2.local", 8002),
        BackendServer::new("backend3.local", 8003),
        BackendServer::new("backend4.local", 8004),
        BackendServer::new("backend5.local", 8005),
    ];

    let mut load_balancer = LoadBalancer::new(servers);

    // Simulate incoming requests
    println!("\n=== Simulating Traffic ===");

    let paths = [
        "/api/users",
        "/api/products",
        "/api/cart",
        "/api/checkout",
        "/",
        "/static/css/main.css",
        "/static/js/app.js",
    ];

    // Generate requests with some having persistent sessions
    let mut persistent_sessions = vec![];
    for i in 0..5 {
        persistent_sessions.push(format!("persistent_session_{}", i));
    }

    // Phase 1: Normal traffic
    println!("\n--- Phase 1: Normal Traffic ---");
    for i in 0..20 {
        let path = paths[i % paths.len()].to_string();
        let client_ip = generate_client_ip();
        let session_id = if i < 10 {
            Some(persistent_sessions[i % persistent_sessions.len()].clone())
        } else {
            generate_session_id()
        };

        let request = HttpRequest {
            path: path.clone(),
            session_id: session_id.clone(),
            client_ip: client_ip.clone(),
        };

        println!("\nRequest: {} from {} (session: {:?})", path, client_ip, session_id);

        let response = load_balancer.route_request(&request);
        println!("Response: {} - {}", response.status, response.body);
    }

    // Phase 2: Simulate server issues and rebalancing
    println!("\n--- Phase 2: Server Issues ---");

    // Simulate high load on some servers
    for server in &load_balancer.servers[0..2] {
        *server.cpu_usage.lock().expect("poisoned mutex") = 0.95;
    }

    load_balancer.rebalance();

    // Continue routing with some servers down
    println!("\n--- Routing with degraded capacity ---");
    for i in 0..10 {
        let path = paths[i % paths.len()].to_string();
        let client_ip = generate_client_ip();
        let session_id = Some(persistent_sessions[i % persistent_sessions.len()].clone());

        let request = HttpRequest {
            path: path.clone(),
            session_id: session_id.clone(),
            client_ip: client_ip.clone(),
        };

        println!("\nRequest: {} (session: {:?})", path, session_id);

        let response = load_balancer.route_request(&request);
        println!("Response: {} - {}", response.status, response.body);
    }

    // Show statistics
    println!("\n=== Server Statistics ===");
    for stats in load_balancer.stats() {
        println!("\n{}: ", stats.id);
        println!("  Status: {}", if stats.healthy { "✓ Healthy" } else { "✗ Unhealthy" });
        println!("  Active Connections: {}", stats.active_connections);
        println!("  Total Requests: {}", stats.total_requests);
        println!("  Avg Response Time: {:?}", stats.avg_response_time);
        println!("  CPU Usage: {:.1}%", stats.cpu_usage * 100.0);
    }

    // Analyze session distribution
    println!("\n=== Session Distribution ===");
    let affinity = load_balancer.session_affinity.lock().expect("poisoned mutex");
    let mut server_sessions: HashMap<String, usize> = HashMap::new();

    for (_, server_id) in affinity.iter() {
        *server_sessions.entry(server_id.clone()).or_insert(0) += 1;
    }

    for (server_id, count) in &server_sessions {
        println!("{}: {} sticky sessions", server_id, count);
    }
}
