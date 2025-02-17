use crate::protobuf::drand::ChainInfoPacket;
use crate::transport::drand::GroupPacket;

pub struct ChainHandler {
    groupfile: Box<GroupPacket>,
}

impl ChainHandler {
    pub fn new(groupfile: GroupPacket) -> Self {
        Self {
            groupfile: groupfile.into(),
        }
    }

    pub fn chain_info(&self) -> ChainInfoPacket {
        self.groupfile.get_chain_info()
    }
}
