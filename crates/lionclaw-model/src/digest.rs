use serde::ser::{
    self, Serialize, SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant,
    SerializeTuple, SerializeTupleStruct, SerializeTupleVariant,
};
use sha2::{Digest, Sha256};

use super::ids::lowercase_hex;
use crate::prelude::*;

pub(crate) struct CanonicalDigest(Sha256);

impl CanonicalDigest {
    pub(crate) fn new(domain: &str) -> Self {
        let mut digest = Self(Sha256::new());
        digest.str("schema", domain);
        digest
    }

    pub(crate) fn str(&mut self, label: &str, value: &str) {
        self.0.update((label.len() as u64).to_be_bytes());
        self.0.update(label.as_bytes());
        self.0.update((value.len() as u64).to_be_bytes());
        self.0.update(value.as_bytes());
    }

    pub(crate) fn bool(&mut self, label: &str, value: bool) {
        self.str(label, if value { "true" } else { "false" });
    }

    pub(crate) fn u64(&mut self, label: &str, value: u64) {
        self.str(label, &value.to_string());
    }

    pub(crate) fn option_u64(&mut self, label: &str, value: Option<u64>) {
        match value {
            Some(value) => self.u64(label, value),
            None => self.str(label, ""),
        }
    }

    pub(crate) fn map<'a, I>(&mut self, label: &str, entries: I)
    where
        I: Iterator<Item = (&'a String, &'a String)>,
    {
        self.str(label, "map");
        for (key, value) in entries {
            self.str("key", key);
            self.str("value", value);
        }
    }

    pub(crate) fn sequence<'a, I, S>(&mut self, label: &str, entries: I)
    where
        I: Iterator<Item = S>,
        S: AsRef<str> + 'a,
    {
        self.str(label, "sequence");
        for entry in entries {
            self.str("item", entry.as_ref());
        }
    }

    pub(crate) fn set<'a, I, S>(&mut self, label: &str, entries: I)
    where
        I: Iterator<Item = S>,
        S: AsRef<str> + 'a,
    {
        self.str(label, "set");
        let mut entries = entries
            .map(|entry| entry.as_ref().to_string())
            .collect::<Vec<_>>();
        entries.sort_unstable();
        for entry in entries {
            self.str("item", &entry);
        }
    }

    pub(crate) fn finish(self) -> String {
        lowercase_hex(&self.0.finalize())
    }
}

pub(crate) fn canonical_serialize_digest<T: Serialize>(
    domain: &str,
    value: &T,
    max_bytes: usize,
) -> Option<String> {
    let mut serializer = CanonicalSerializer {
        digest: Sha256::new(),
        encoded_len: 0,
    };
    serializer.feed(b"domain", domain.as_bytes());
    value.serialize(&mut serializer).ok()?;
    (serializer.encoded_len <= max_bytes).then(|| lowercase_hex(&serializer.digest.finalize()))
}

struct CanonicalSerializer {
    digest: Sha256,
    encoded_len: usize,
}

impl CanonicalSerializer {
    fn feed(&mut self, tag: &[u8], bytes: &[u8]) {
        self.encoded_len = self
            .encoded_len
            .saturating_add(16)
            .saturating_add(tag.len())
            .saturating_add(bytes.len());
        self.digest.update((tag.len() as u64).to_be_bytes());
        self.digest.update(tag);
        self.digest.update((bytes.len() as u64).to_be_bytes());
        self.digest.update(bytes);
    }

    fn marker(&mut self, tag: &'static [u8]) {
        self.feed(tag, &[]);
    }

    fn length(&mut self, tag: &'static [u8], len: Option<usize>) {
        match len {
            Some(len) => self.feed(tag, &(len as u64).to_be_bytes()),
            None => self.marker(tag),
        }
    }
}

#[derive(Debug)]
struct CanonicalSerializeError;

impl core::fmt::Display for CanonicalSerializeError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str("canonical serialization failed")
    }
}

impl core::error::Error for CanonicalSerializeError {}

impl ser::Error for CanonicalSerializeError {
    fn custom<T: core::fmt::Display>(_message: T) -> Self {
        Self
    }
}

struct CanonicalCompound<'a>(&'a mut CanonicalSerializer);

impl<'a> ser::Serializer for &'a mut CanonicalSerializer {
    type Ok = ();
    type Error = CanonicalSerializeError;
    type SerializeSeq = CanonicalCompound<'a>;
    type SerializeTuple = CanonicalCompound<'a>;
    type SerializeTupleStruct = CanonicalCompound<'a>;
    type SerializeTupleVariant = CanonicalCompound<'a>;
    type SerializeMap = CanonicalCompound<'a>;
    type SerializeStruct = CanonicalCompound<'a>;
    type SerializeStructVariant = CanonicalCompound<'a>;

    fn serialize_bool(self, value: bool) -> Result<Self::Ok, Self::Error> {
        self.feed(b"bool", &[u8::from(value)]);
        Ok(())
    }

    fn serialize_i8(self, value: i8) -> Result<Self::Ok, Self::Error> {
        self.feed(b"i8", &value.to_be_bytes());
        Ok(())
    }

    fn serialize_i16(self, value: i16) -> Result<Self::Ok, Self::Error> {
        self.feed(b"i16", &value.to_be_bytes());
        Ok(())
    }

    fn serialize_i32(self, value: i32) -> Result<Self::Ok, Self::Error> {
        self.feed(b"i32", &value.to_be_bytes());
        Ok(())
    }

    fn serialize_i64(self, value: i64) -> Result<Self::Ok, Self::Error> {
        self.feed(b"i64", &value.to_be_bytes());
        Ok(())
    }

    fn serialize_i128(self, value: i128) -> Result<Self::Ok, Self::Error> {
        self.feed(b"i128", &value.to_be_bytes());
        Ok(())
    }

    fn serialize_u8(self, value: u8) -> Result<Self::Ok, Self::Error> {
        self.feed(b"u8", &value.to_be_bytes());
        Ok(())
    }

    fn serialize_u16(self, value: u16) -> Result<Self::Ok, Self::Error> {
        self.feed(b"u16", &value.to_be_bytes());
        Ok(())
    }

    fn serialize_u32(self, value: u32) -> Result<Self::Ok, Self::Error> {
        self.feed(b"u32", &value.to_be_bytes());
        Ok(())
    }

    fn serialize_u64(self, value: u64) -> Result<Self::Ok, Self::Error> {
        self.feed(b"u64", &value.to_be_bytes());
        Ok(())
    }

    fn serialize_u128(self, value: u128) -> Result<Self::Ok, Self::Error> {
        self.feed(b"u128", &value.to_be_bytes());
        Ok(())
    }

    fn serialize_f32(self, value: f32) -> Result<Self::Ok, Self::Error> {
        self.feed(b"f32", &value.to_bits().to_be_bytes());
        Ok(())
    }

    fn serialize_f64(self, value: f64) -> Result<Self::Ok, Self::Error> {
        self.feed(b"f64", &value.to_bits().to_be_bytes());
        Ok(())
    }

    fn serialize_char(self, value: char) -> Result<Self::Ok, Self::Error> {
        let mut encoded = [0; 4];
        self.feed(b"char", value.encode_utf8(&mut encoded).as_bytes());
        Ok(())
    }

    fn serialize_str(self, value: &str) -> Result<Self::Ok, Self::Error> {
        self.feed(b"str", value.as_bytes());
        Ok(())
    }

    fn serialize_bytes(self, value: &[u8]) -> Result<Self::Ok, Self::Error> {
        self.feed(b"bytes", value);
        Ok(())
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        self.marker(b"none");
        Ok(())
    }

    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<Self::Ok, Self::Error> {
        self.marker(b"some");
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        self.marker(b"unit");
        Ok(())
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        self.feed(b"unit_struct", name.as_bytes());
        Ok(())
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        self.feed(b"enum", name.as_bytes());
        self.feed(b"variant_index", &variant_index.to_be_bytes());
        self.feed(b"variant", variant.as_bytes());
        Ok(())
    }

    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        self.feed(b"newtype_struct", name.as_bytes());
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        self.feed(b"enum", name.as_bytes());
        self.feed(b"variant_index", &variant_index.to_be_bytes());
        self.feed(b"variant", variant.as_bytes());
        value.serialize(self)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        self.length(b"seq", len);
        Ok(CanonicalCompound(self))
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        self.length(b"tuple", Some(len));
        Ok(CanonicalCompound(self))
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        self.feed(b"tuple_struct", name.as_bytes());
        self.length(b"len", Some(len));
        Ok(CanonicalCompound(self))
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        self.serialize_unit_variant(name, variant_index, variant)?;
        self.length(b"tuple_variant", Some(len));
        Ok(CanonicalCompound(self))
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        self.length(b"map", len);
        Ok(CanonicalCompound(self))
    }

    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        self.feed(b"struct", name.as_bytes());
        self.length(b"len", Some(len));
        Ok(CanonicalCompound(self))
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        self.serialize_unit_variant(name, variant_index, variant)?;
        self.length(b"struct_variant", Some(len));
        Ok(CanonicalCompound(self))
    }
}

impl SerializeSeq for CanonicalCompound<'_> {
    type Ok = ();
    type Error = CanonicalSerializeError;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        value.serialize(&mut *self.0)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.marker(b"end_seq");
        Ok(())
    }
}

impl SerializeTuple for CanonicalCompound<'_> {
    type Ok = ();
    type Error = CanonicalSerializeError;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        value.serialize(&mut *self.0)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.marker(b"end_tuple");
        Ok(())
    }
}

impl SerializeTupleStruct for CanonicalCompound<'_> {
    type Ok = ();
    type Error = CanonicalSerializeError;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        value.serialize(&mut *self.0)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.marker(b"end_tuple_struct");
        Ok(())
    }
}

impl SerializeTupleVariant for CanonicalCompound<'_> {
    type Ok = ();
    type Error = CanonicalSerializeError;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        value.serialize(&mut *self.0)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.marker(b"end_tuple_variant");
        Ok(())
    }
}

impl SerializeMap for CanonicalCompound<'_> {
    type Ok = ();
    type Error = CanonicalSerializeError;

    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<(), Self::Error> {
        self.0.marker(b"map_key");
        key.serialize(&mut *self.0)
    }

    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.0.marker(b"map_value");
        value.serialize(&mut *self.0)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.marker(b"end_map");
        Ok(())
    }
}

impl SerializeStruct for CanonicalCompound<'_> {
    type Ok = ();
    type Error = CanonicalSerializeError;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        self.0.feed(b"field", key.as_bytes());
        value.serialize(&mut *self.0)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.marker(b"end_struct");
        Ok(())
    }
}

impl SerializeStructVariant for CanonicalCompound<'_> {
    type Ok = ();
    type Error = CanonicalSerializeError;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        self.0.feed(b"field", key.as_bytes());
        value.serialize(&mut *self.0)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.marker(b"end_struct_variant");
        Ok(())
    }
}
