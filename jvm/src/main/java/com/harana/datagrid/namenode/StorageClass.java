package com.harana.datagrid.namenode;

import com.harana.datagrid.conf.CrailConstants;
import com.harana.datagrid.metadata.BlockInfo;
import com.harana.datagrid.metadata.DataNodeInfo;
import com.harana.datagrid.rpc.RpcErrors;
import com.harana.datagrid.utils.AtomicIntegerModulo;
import com.harana.datagrid.utils.CrailUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StorageClass {
    private static final Logger logger = LogManager.getLogger();

    private int storageClass;
    private ConcurrentHashMap<Long, DataNodeBlocks> membership;
    private ConcurrentHashMap<Integer, DataNodeArray> affinitySets;
    private DataNodeArray anySet;
    private BlockSelection blockSelection;

    public StorageClass(int storageClass){
        this.storageClass = storageClass;
        this.membership = new ConcurrentHashMap<Long, DataNodeBlocks>();
        this.affinitySets = new ConcurrentHashMap<Integer, DataNodeArray>();
        if (CrailConstants.NAMENODE_BLOCKSELECTION.equalsIgnoreCase("roundrobin")){
            this.blockSelection = new RoundRobinBlockSelection();
        } else {
            this.blockSelection = new RandomBlockSelection();
        }
        this.anySet = new DataNodeArray(blockSelection);
    }

    public short updateRegion(BlockInfo region) {
        long dnAddress = region.getDnInfo().key();
        DataNodeBlocks current = membership.get(dnAddress);
        if (current == null) {
            return RpcErrors.ERR_ADD_BLOCK_FAILED;
        } else {
            return current.updateRegion(region);
        }
    }

    public boolean regionExists(BlockInfo region) {
        long dnAddress = region.getDnInfo().key();
        DataNodeBlocks current = membership.get(dnAddress);
        if (current == null) {
            return false;
        } else {
            return current.regionExists(region);
        }
    }

    short addBlock(NameNodeBlockInfo block) throws UnknownHostException {
        long dnAddress = block.getDnInfo().key();
        DataNodeBlocks current = membership.get(dnAddress);
        if (current == null) {
            current = DataNodeBlocks.fromDataNodeInfo(block.getDnInfo());
            addDataNode(current);
        }

        current.touch();
        current.addFreeBlock(block);
        return RpcErrors.ERR_OK;
    }

    NameNodeBlockInfo getBlock(int affinity) throws InterruptedException {
        NameNodeBlockInfo block;
        if (affinity == 0) {
            block = anySet.get();
        } else {
            block = _getAffinityBlock(affinity);
            if (block == null) block = anySet.get();
        }
        return block;
    }

    DataNodeBlocks getDataNode(DataNodeInfo dataNode) {
        return membership.get(dataNode.key());
    }

    short addDataNode(DataNodeBlocks dataNode) {
        DataNodeBlocks current = membership.putIfAbsent(dataNode.key(), dataNode);
        if (current != null) {
            return RpcErrors.ERR_DATANODE_NOT_REGISTERED;
        }

        // current == null, datanode not in set, adding it now
        _addDataNode(dataNode);
        return RpcErrors.ERR_OK;
    }


    private void _addDataNode(DataNodeBlocks dataNode){
        logger.info("adding datanode " + CrailUtils.getIPAddressFromBytes(dataNode.getIpAddress()) + ":" + dataNode.getPort() + " of type " + dataNode.getStorageType() + " to storage class " + storageClass);
        DataNodeArray hostMap = affinitySets.get(dataNode.getLocationClass());
        if (hostMap == null){
            hostMap = new DataNodeArray(blockSelection);
            DataNodeArray oldMap = affinitySets.putIfAbsent(dataNode.getLocationClass(), hostMap);
            if (oldMap != null){
                hostMap = oldMap;
            }
        }
        hostMap.add(dataNode);
        anySet.add(dataNode);
    }

    private NameNodeBlockInfo _getAffinityBlock(int affinity) throws InterruptedException {
        NameNodeBlockInfo block = null;
        DataNodeArray affinitySet = affinitySets.get(affinity);
        if (affinitySet != null){
            block = affinitySet.get();
        }
        return block;
    }

    public interface BlockSelection {
        int getNext(int size);
    }

    private static class RoundRobinBlockSelection implements BlockSelection {
        private AtomicIntegerModulo counter;

        public RoundRobinBlockSelection(){
            logger.info("round robin block selection");
            counter = new AtomicIntegerModulo();
        }

        @Override
        public int getNext(int size) {
            return counter.getAndIncrement() % size;
        }
    }

    private class RandomBlockSelection implements BlockSelection {
        public RandomBlockSelection(){
            logger.info("random block selection");
        }

        @Override
        public int getNext(int size) {
            return ThreadLocalRandom.current().nextInt(size);
        }
    }

    private class DataNodeArray {
        private ArrayList<DataNodeBlocks> arrayList;
        private ReentrantReadWriteLock lock;
        private BlockSelection blockSelection;

        public DataNodeArray(BlockSelection blockSelection){
            this.arrayList = new ArrayList<DataNodeBlocks>();
            this.lock = new ReentrantReadWriteLock();
            this.blockSelection = blockSelection;
        }

        public void add(DataNodeBlocks dataNode){
            lock.writeLock().lock();
            try {
                arrayList.add(dataNode);
            } finally {
                lock.writeLock().unlock();
            }
        }

        private NameNodeBlockInfo get() {
            lock.readLock().lock();
            try {
                NameNodeBlockInfo block = null;
                int size = arrayList.size();
                if (size > 0){
                    int startIndex = blockSelection.getNext(size);
                    for (int i = 0; i < size; i++){
                        int index = (startIndex + i) % size;
                        DataNodeBlocks anyDn = arrayList.get(index);
                        if (anyDn.isOnline()){
                            block = anyDn.getFreeBlock();
                        }
                        if (block != null){
                            break;
                        }
                    }
                }
                return block;
            } finally {
                lock.readLock().unlock();
            }
        }
    }
}