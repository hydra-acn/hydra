use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use rand::Rng;
use std::convert::{TryFrom, TryInto};

use crate::crypto::cprng::thread_cprng;
use crate::defs::{CircuitId, RoundNo, Token, CELL_LEN, ONION_LEN};
use crate::error::Error;
use crate::mix::rss_pipeline::Scalable;

const CID_LEN: usize = std::mem::size_of::<CircuitId>();
const RNO_LEN: usize = std::mem::size_of::<RoundNo>();

#[derive(Debug, Default, Clone, PartialEq)]
pub struct Cell(Vec<u8>);

impl Cell {
    pub fn dummy(cid: CircuitId, r: RoundNo) -> Self {
        let vec = vec![0; CELL_LEN];
        let mut cell: Self = vec.try_into().unwrap();
        cell.set_circuit_id(cid);
        cell.set_round_no(r);
        cell.randomize();
        cell
    }

    pub fn circuit_id(&self) -> CircuitId {
        let len = self.0.len();
        LittleEndian::read_u64(&self.0[len - CID_LEN - RNO_LEN..len - RNO_LEN])
    }

    pub fn set_circuit_id(&mut self, id: CircuitId) {
        let len = self.0.len();
        LittleEndian::write_u64(&mut self.0[len - CID_LEN - RNO_LEN..len - RNO_LEN], id);
    }

    pub fn round_no(&self) -> RoundNo {
        let len = self.0.len();
        LittleEndian::read_u32(&self.0[len - RNO_LEN..])
    }

    pub fn set_round_no(&mut self, r: RoundNo) {
        let len = self.0.len();
        LittleEndian::write_u32(&mut self.0[len - RNO_LEN..], r);
    }

    pub fn onion(&self) -> &[u8] {
        let len = self.0.len();
        &self.0[0..len - CID_LEN - RNO_LEN]
    }

    pub fn onion_mut(&mut self) -> &mut [u8] {
        let len = self.0.len();
        &mut self.0[0..len - CID_LEN - RNO_LEN]
    }

    pub fn command(&self) -> Option<CellCmd> {
        read_command(&self.0[0..8])
    }

    pub fn set_command(&mut self, cmd: CellCmd) {
        set_command(cmd, &mut self.0[0..8]);
    }

    pub fn token(&self) -> Token {
        LittleEndian::read_u64(&self.0[8..16])
    }

    pub fn set_token(&mut self, token: Token) {
        LittleEndian::write_u64(&mut self.0[8..16], token)
    }

    /// Turn existing cell into dummy by randomizing the onion encrypted part.
    pub fn randomize(&mut self) {
        thread_cprng().fill(self.onion_mut());
    }

    pub fn buf(&self) -> &[u8] {
        &self.0
    }

    /// Consume the cell and return only the onion part as byte vector.
    pub fn into_onion(mut self) -> Vec<u8> {
        self.0.truncate(ONION_LEN);
        self.0
    }
}

pub enum CellCmd {
    Delay(u8),
    Subscribe(u8),
    Broadcast,
}

/// Read the 8 bytes (args, cmd) of cells and return the `CellCmd` if there is one.
pub fn read_command(slice: &[u8]) -> Option<CellCmd> {
    if slice.iter().all(|b| *b == 255) {
        Some(CellCmd::Broadcast)
    } else if slice[2..].iter().all(|b| *b == 0) {
        match slice[1] {
            0 => Some(CellCmd::Delay(slice[0])),
            1 => Some(CellCmd::Subscribe(slice[0])),
            _ => None,
        }
    } else {
        None
    }
}

/// Set the 8 bytes (args, cmd) of cells based on the given `CellCmd`.
pub fn set_command(cmd: CellCmd, slice: &mut [u8]) {
    match cmd {
        CellCmd::Delay(rounds) => {
            for b in slice.iter_mut() {
                *b = 0;
            }
            slice[0] = rounds;
        }
        CellCmd::Subscribe(n_tokens) => {
            for b in slice.iter_mut() {
                *b = 0;
            }
            slice[1] = 1;
            slice[0] = n_tokens;
        }
        CellCmd::Broadcast => {
            for b in slice.iter_mut() {
                *b = 255;
            }
        }
    }
}

impl Scalable for Cell {
    fn thread_id(&self, size: usize) -> usize {
        self.circuit_id() as usize % size
    }
}

impl TryFrom<Vec<u8>> for Cell {
    type Error = Error;
    fn try_from(vec: Vec<u8>) -> Result<Self, Self::Error> {
        if vec.len() < CID_LEN + RNO_LEN {
            return Err(Self::Error::InputError("Vector too short".to_string()));
        }
        let mut cell = Cell::default();
        cell.0 = vec;
        Ok(cell)
    }
}

impl From<crate::tonic_mix::Cell> for Cell {
    fn from(mut cell: crate::tonic_mix::Cell) -> Self {
        cell.onion
            .write_u64::<LittleEndian>(cell.circuit_id)
            .unwrap();
        cell.onion.write_u32::<LittleEndian>(cell.round_no).unwrap();
        cell.onion.try_into().unwrap()
    }
}
