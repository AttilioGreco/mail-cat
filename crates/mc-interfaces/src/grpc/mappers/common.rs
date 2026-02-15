use crate::grpc::proto::{self, Flag as ProtoFlag, Timestamp};
use chrono::{DateTime, Utc};
use mc_domain::value_objects::flag::Flag as DomainFlag;

/// Convert chrono DateTime to proto Timestamp.
pub fn datetime_to_timestamp(dt: &DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    }
}

/// Convert proto Timestamp to chrono DateTime.
pub fn timestamp_to_datetime(ts: &Timestamp) -> DateTime<Utc> {
    DateTime::from_timestamp(ts.seconds, ts.nanos as u32).unwrap_or_default()
}

/// Convert domain Flag to proto Flag.
pub fn domain_flag_to_proto(flag: &DomainFlag) -> i32 {
    match flag {
        DomainFlag::Seen => ProtoFlag::Seen as i32,
        DomainFlag::Answered => ProtoFlag::Answered as i32,
        DomainFlag::Flagged => ProtoFlag::Flagged as i32,
        DomainFlag::Deleted => ProtoFlag::Deleted as i32,
        DomainFlag::Draft => ProtoFlag::Draft as i32,
        DomainFlag::Recent => ProtoFlag::Recent as i32,
        DomainFlag::Custom(_) => ProtoFlag::Unspecified as i32, // Custom flags not supported in proto
    }
}

/// Convert proto Flag to domain Flag.
pub fn proto_flag_to_domain(flag: i32) -> Option<DomainFlag> {
    match ProtoFlag::try_from(flag).ok()? {
        ProtoFlag::Unspecified => None,
        ProtoFlag::Seen => Some(DomainFlag::Seen),
        ProtoFlag::Answered => Some(DomainFlag::Answered),
        ProtoFlag::Flagged => Some(DomainFlag::Flagged),
        ProtoFlag::Deleted => Some(DomainFlag::Deleted),
        ProtoFlag::Draft => Some(DomainFlag::Draft),
        ProtoFlag::Recent => Some(DomainFlag::Recent),
    }
}

/// Convert domain Address to proto EmailAddress.
pub fn domain_address_to_proto(addr: &mc_domain::model::message::Address) -> proto::EmailAddress {
    proto::EmailAddress {
        email: addr.email.as_str().to_string(),
        name: addr.name.clone(),
    }
}

/// Convert proto EmailAddress to domain Address.
pub fn proto_email_to_domain(
    email: &proto::EmailAddress,
) -> Result<mc_domain::model::message::Address, String> {
    Ok(mc_domain::model::message::Address {
        name: email.name.clone(),
        email: mc_domain::value_objects::email_address::EmailAddress::new(&email.email)
            .map_err(|e| format!("Invalid email: {}", e))?,
    })
}
