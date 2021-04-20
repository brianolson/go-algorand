#!/usr/bin/env python3
#
# protoc t.proto --python_out=.

import json
import logging
import os
import sys

import algosdk
#import algosdk.encoding.msgpack as msgpack
msgpack = algosdk.encoding.msgpack
from algosdk.v2client.algod import AlgodClient
import google.protobuf.json_format
# google.protobuf.json_format.MessageToJson(pb ob)
# google.protobuf.json_format.Parse('json', pb ob)

import t_pb2 as tpb

logger = logging.getLogger(__name__)

# map msgpack dict name to protobuf attr name
SignedTxnInBlk = {
    'hgi':'has_genesis_id',
    'hgh':'has_genesis_hash',
    'txn':('txn', tpb.Transaction),
    'sig':'sig',
    'msig':('msig', tpb.MultisigSig),
    'lsig':('lsig', tpb.LogicSig),
    'sigr':'auth_addr',
    'ca':'apply_data.closing_amount',
    'aca':'apply_data.asset_closing_amount',
    'rs':'apply_data.sender_rewards',
    'rr':'apply_data.receiver_rewards',
    'rc':'apply_data.close_rewards',
    'dt':('apply_data.eval', tpb.EvalData),
}

mappings = {
    tpb.SignedTxnInBlock: SignedTxnInBlk,
    tpb.Transaction:{
        'snd':'sender',
        'fee':'fee',
        'fv':'first_valid',
        'lv':'last_valid',
        'note':'note',
        'gen':'genesis_id',
        'gh':'genesis_hash',
        'grp':'txgroup',
        'lx':'lease',
        'rekey':'rekey_to',
        'type':'type',

        'amt':'payment.amount',
        'rcv':'payment.receiver',
        'close':'payment.close_remainder_to',
    },
    tpb.MultisigSig:{
        'v':'version',
        'thr':'threshold',
        'subsig':('subsigs', tpb.MultisigSubsig),
    },
    tpb.MultisigSubsig:{
        'pk':'key',
        's':'signature',
    },
}

def pathset(out, fm, v):
    path = fm.split('.')
    if len(path) > 1:
        for pk in path[:-1]:
            out = getattr(out, pk)
    try:
        setattr(out, path[-1], v)
    except:
        logger.error('could not set .%s of <%s> %r', path[-1], type(out), out, exc_info=True)
        raise

def dict_to_pb(d, clazz):
    out = clazz()
    #for k, fm in field_map.items():
    #    v = d.get(k)
    dict_to_pbob(d, out, clazz)
    return out

def dict_to_pbob(d, out, clazz):
    field_map = mappings[clazz]
    if isinstance(d, (list,tuple)):
        for v in d:
            out.append(dict_to_pb(v, clazz))
        return
    for k, v in d.items():
        # if v is None:
        #     continue
        fm = field_map[k]
        if isinstance(fm, str):
            pathset(out, fm, v)
            continue
        if isinstance(fm, tuple):
            attrname, nclazz = fm
            dict_to_pbob(v, getattr(out, attrname), nclazz)
            continue
        raise Exception("don't know what to do for d[{}] -> {}.{}".format(k, clazz, fm))

# input, block['block']['txns'][n] SignedTxnInBlk
def to_pb(stxn):
    return dict_to_pb(stxn, tpb.SignedTxnInBlock)


def token_addr_from_args(args):
    if args.algod:
        addr = open(os.path.join(args.algod, 'algod.net'), 'rt').read().strip()
        token = open(os.path.join(args.algod, 'algod.token'), 'rt').read().strip()
    else:
        addr = args.algod_net
        token = args.algod_token
    if not addr.startswith('http'):
        addr = 'http://' + addr
    return token, addr

def wat():
    return

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('-d', '--algod', default=None, help='algorand data dir')
    ap.add_argument('--algod-net', default=None, help='algod host:port')
    ap.add_argument('--algod-token', default=None, help='algod token')
    ap.add_argument('--txn-limit', default=None, type=int, help='stop after some number of txn')
    ap.add_argument('--block-start', default=0, type=int, help='block number to start at')
    ap.add_argument('--verbose', default=False, action='store_true')
    args = ap.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    token, addr = token_addr_from_args(args)
    algod = AlgodClient(token, addr)

    block_num = args.block_start
    txncount = 0
    sizes = []
    while True:
        try:
            block = algod.block_info(block_num, response_format='msgpack')
        except:
            return 1
        block = algosdk.encoding.msgpack.unpackb(block)
        #print(block['block'].keys())
        #print('r={} len(txns)={}'.format(block_num, len(block['block'].get('txns',[]))))
        intra = 0
        for stxn in block['block'].get('txns',[]):
            #print('{}:{}'.format(block_num, intra))
            mpbytes = msgpack.packb(stxn)
            pbstxn = to_pb(stxn)
            pbbytes = pbstxn.SerializeToString()
            # TODO: reverse the encoding translation, pbbytes back to mppbytes to ensure that all the data is there and canonical msgpack can still be created
            sizes.append((block_num, intra, len(mpbytes), len(pbbytes)))
            print('{}:{}\t{}\t{}'.format(block_num, intra, len(mpbytes), len(pbbytes)))
            intra += 1
            txncount += 1
            if (args.txn_limit is not None) and (txncount >= args.txn_limit):
                break
        block_num += 1
        if (args.txn_limit is not None) and (txncount >= args.txn_limit):
            break
    pbtot = 0
    mptot = 0
    count = 0
    for block_num, intra, mplen, pblen in sizes:
        mptot += mplen
        pbtot += pblen
        count += 1
    print('N={}, avg msgp {}, avg pb {}'.format(count, mptot/count, pbtot/count))
    return 0

if __name__ == '__main__':
    sys.exit(main())
