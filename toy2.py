#!/usr/bin/env python

import os
import sys

import torch as th
import torch.distributed as dist

rabbitUri = ''

PS_JOB_NAME = "ps"
WORKER_JOB_NAME = "worker"


# FLAGS and unparsed declared below configure_parse_arguments()


def configure_parse_arguments():
    import argparse
    parser = argparse.ArgumentParser()
    parser.register("type", "bool", lambda v: v.lower() == "true")
    parser.add_argument("--ps_hosts", type=str, default="",
                        help="Comma-separated list of hostname:port pairs")
    parser.add_argument("--worker_hosts", type=str, default="",
                        help="Comma-separated list of hostname:port pairs")
    parser.add_argument("--job_name", type=str, default="", help="One of 'ps', 'worker'")
    parser.add_argument("--task_index", type=int, default=0, help="Index of task within the job")
    parser.add_argument("--data_set_path", type=str, default="", help="Path of the dataset to load")
    parser.add_argument("--checkpoint_path", type=str, default="",
                        help="Path for saving checkpoints")
    parser.add_argument("--restore_file_path", type=str, default="",
                        help="Path to file containing parameters for model restore")
    parser.add_argument("--experiment_id", type=str, default="", help="ID of experiment")
    parser.add_argument("--rabbit_uri", type=str, default="",
                        help="URI of RabbitMQ server, experiment id must be defined")
    return parser.parse_known_args()


FLAGS, unparsed = configure_parse_arguments()


def allreduce(send, recv):
    """ Implementation of a ring-reduce. """
    rank = dist.get_rank()
    size = dist.get_world_size()
    send_buff = th.zeros(send.size())
    recv_buff = th.zeros(send.size())
    accum = th.zeros(send.size())
    accum[:] = send[:]
    # th.cuda.synchronize()

    left = ((rank - 1) + size) % size
    right = (rank + 1) % size

    for i in range(size - 1):
        if i % 2 == 0:
            # Send send_buff
            send_req = dist.isend(send_buff, right)
            dist.recv(recv_buff, left)
            accum[:] += recv[:]
        else:
            # Send recv_buff
            send_req = dist.isend(recv_buff, right)
            dist.recv(send_buff, left)
            accum[:] += send[:]
        send_req.wait()
    # th.cuda.synchronize()
    recv[:] = accum[:]


def run(rank, size):
    """ Distributed function to be implemented later. """
    #    t = th.ones(2, 2)
    t = th.rand(2, 2).cuda()
    # for _ in range(10000000):
    for _ in range(4):
        c = t.clone()
        dist.all_reduce(c, dist.reduce_op.SUM)
        #        allreduce(t, c)
        t.set_(c)
    print(t)


def main(argv):
    job_name = FLAGS.job_name
    master_addrs, master_port = FLAGS.ps_hosts.split(':')
    os.environ['MASTER_ADDR'] = master_addrs
    os.environ['MASTER_PORT'] = master_port
    if job_name == PS_JOB_NAME:
        print('no idea')
    elif job_name == WORKER_JOB_NAME:
        world = FLAGS.worker_hosts.split(',')
        init_processes(FLAGS.task_index, len(world), run)


def init_processes(rank, size, fn, backend='mpi'):
    """ Initialize the distributed environment. """
    dist.init_process_group(backend, rank=rank, world_size=size)
    # dist.init_process_group(backend, world_size=size)
    fn(rank, size)


def entry(datasets=None, checkpoint_path=None, restore_file_path=None):
    print(f"entry: datasets {datasets} , checkpoint_path {checkpoint_path}")

    FLAGS.ps_hosts = os.environ['PS_HOSTS']
    FLAGS.worker_hosts = os.environ['WORKER_HOSTS']
    FLAGS.job_name = os.environ['JOB_NAME']
    FLAGS.task_index = int(os.environ['TASK_INDEX'])
    if datasets is not None:
        FLAGS.data_set_path = datasets
    if checkpoint_path is not None:
        FLAGS.checkpoint_path = checkpoint_path
    if restore_file_path is not None:
        FLAGS.restore_file_path = restore_file_path

    print(FLAGS)
    main(argv=[sys.argv[0]] + unparsed)


if __name__ == "__main__":
    entry()
