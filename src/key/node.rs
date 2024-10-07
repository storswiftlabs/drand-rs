use super::keys::Identity;
use energon::drand::Scheme;

#[derive(PartialEq)]
pub struct Node<S: Scheme> {
    identity: Identity<S>,
    index: u32,
}

impl<S: Scheme> Node<S> {
    pub fn into_parts(self) -> (Identity<S>, u32) {
        (self.identity, self.index)
    }

    pub fn new(identity: Identity<S>, index: u32) -> Self {
        Self { identity, index }
    }

    pub fn identity(&self) -> &Identity<S> {
        &self.identity
    }

    pub fn index(&self) -> u32 {
        self.index
    }
}
