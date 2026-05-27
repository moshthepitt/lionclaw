use anyhow::{anyhow, bail, Result};

pub const CHANNEL_ID: &str = "team-local";

const PEER_PREFIX: &str = "team-local:peer:";
const INSTANCE_PREFIX: &str = "team-local:instance:";
const MESSAGE_PREFIX: &str = "team-local:delivery:";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryRoute {
    HomeId(String),
    InstanceName(String),
}

pub fn parse_delivery_route(raw: &str) -> Result<DeliveryRoute> {
    let raw = raw.trim();
    if let Some(home_id) = raw.strip_prefix(PEER_PREFIX) {
        let home_id = required_component(home_id, "team-local peer home id")?;
        return Ok(DeliveryRoute::HomeId(home_id.to_string()));
    }
    if let Some(instance_name) = raw.strip_prefix(INSTANCE_PREFIX) {
        let instance_name = required_component(instance_name, "team-local instance name")?;
        return Ok(DeliveryRoute::InstanceName(instance_name.to_string()));
    }
    bail!("unsupported team-local route '{raw}'");
}

pub fn peer_conversation_ref(home_id: &str) -> String {
    format!("{PEER_PREFIX}{home_id}")
}

pub fn sender_ref(home_id: &str) -> String {
    format!("{INSTANCE_PREFIX}{home_id}")
}

pub fn inbound_event_id(sender_home_id: &str, delivery_id: &str) -> String {
    format!("{CHANNEL_ID}:{sender_home_id}:{delivery_id}")
}

pub fn inbound_message_ref(delivery_id: &str) -> String {
    format!("{MESSAGE_PREFIX}{delivery_id}")
}

pub fn provider_file_ref(delivery_id: &str, attachment_id: &str) -> String {
    format!("{MESSAGE_PREFIX}{delivery_id}:attachment:{attachment_id}")
}

fn required_component<'a>(value: &'a str, label: &str) -> Result<&'a str> {
    let value = value.trim();
    if value.is_empty() {
        return Err(anyhow!("{label} is required"));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::{
        inbound_event_id, inbound_message_ref, parse_delivery_route, peer_conversation_ref,
        provider_file_ref, sender_ref, DeliveryRoute,
    };

    #[test]
    fn parses_home_id_routes() {
        assert_eq!(
            parse_delivery_route("team-local:peer:home-1").expect("route"),
            DeliveryRoute::HomeId("home-1".to_string())
        );
    }

    #[test]
    fn parses_instance_routes() {
        assert_eq!(
            parse_delivery_route("team-local:instance:reviewer").expect("route"),
            DeliveryRoute::InstanceName("reviewer".to_string())
        );
    }

    #[test]
    fn constructs_stable_refs_from_home_ids_and_delivery_ids() {
        assert_eq!(
            peer_conversation_ref("home-reviewer"),
            "team-local:peer:home-reviewer"
        );
        assert_eq!(sender_ref("home-main"), "team-local:instance:home-main");
        assert_eq!(
            inbound_event_id("home-main", "delivery-1"),
            "team-local:home-main:delivery-1"
        );
        assert_eq!(
            inbound_message_ref("delivery-1"),
            "team-local:delivery:delivery-1"
        );
        assert_eq!(
            provider_file_ref("delivery-1", "att-1"),
            "team-local:delivery:delivery-1:attachment:att-1"
        );
    }
}
