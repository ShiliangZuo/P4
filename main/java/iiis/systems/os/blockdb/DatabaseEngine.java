package iiis.systems.os.blockdb;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.json.JSONObject;
import org.omg.CORBA.DynAnyPackage.Invalid;

import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.regex.Pattern;

public class DatabaseEngine {
    private static DatabaseEngine instance = null;

    public static DatabaseEngine getInstance() {
        return instance;
    }

    public static void setup(String dataDir, int id, JSONObject config) {
        instance = new DatabaseEngine(dataDir, id, config);
    }

    //A hashmap that returns default 1000 if key not found
    private DefaultHashMap<String, Integer> balances = new DefaultHashMap<>(1000);
    private String dataDir;

    //The id of this server
    private int serverId;
    private JSONObject configJson;
    private int nServers;

    private int N = 50;

    private Semaphore semaphore = new Semaphore(1);

    //Transactions that have been written in a block
    private HashSet<String> transactionRecords = new HashSet<>();

    //Transactions that have not been written in a block
    private LinkedList<Transaction> pendingTransactions = new LinkedList<>();

    //The structure of block tree
    private HashMap<String, Block> blockTree = new HashMap<>();

    //Maintains the longest chain
    private LinkedList<Block> blockChain = new LinkedList<>();

    //Use regular expression to match username
    final int userIdLength = 8;
    final String template = "[a-z0-9|-]{" + userIdLength + "}";
    Pattern pattern = Pattern.compile(template, Pattern.CASE_INSENSITIVE);

    //Used for communication
    private static ManagedChannel channel;
    private static BlockChainMinerGrpc.BlockChainMinerBlockingStub blockingStub;
    private static BlockChainMinerGrpc.BlockChainMinerStub asyncStub;

    private static String ZEROSTRING = "0000000000000000000000000000000000000000000000000000000000000000";

    private boolean mined = false, changed = false;
    private Thread mining = new Mining(pendingTransactions, balances);

    DatabaseEngine(String dataDir, int id, JSONObject config) {
        this.dataDir = dataDir;
        this.serverId = id;
        this.configJson = config;
        this.nServers = config.getInt("nservers");
    }

    private int getOrZero(String userId) {
        /*if (balances.containsKey(userId)) {
            return balances.get(userId);
        } else {
            balances.put(userId, 1000);
        }*/
        if (balances.containsKey(userId) == false) {
            balances.put(userId, 1000);
        }
        return balances.get(userId);
    }

    public int get(String userId) {
        //logLength++;
        try {
            semaphore.acquire();
            int value = getOrZero(userId);
            semaphore.release();
            return value;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    public boolean transfer(Transaction request) {
        Transaction.Types type = request.getType();
        String fromId = request.getFromID();
        String toId = request.getToID();
        int value = request.getValue();
        int fee = request.getMiningFee();
        String UUID = request.getUUID();
        try {
            semaphore.acquire();
            if (type == Transaction.Types.TRANSFER && pattern.matcher(fromId).matches()
                    && pattern.matcher(toId).matches() && !fromId.equals(toId) && value >= 0
                    && fee >= 0 && !transactionRecords.contains(request.getUUID())) {
                int fromBalance = getOrZero(fromId);
                if (value <= fromBalance && value >= fee) {
                    int toBalance = getOrZero(toId);
                    balances.put(fromId, fromBalance - value);
                    balances.put(toId, toBalance + value - fee);

                    semaphore.release();
                    return true;
                }
            }
            semaphore.release();
            return false;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    // Broadcast a request to other servers
    public boolean broadcast(Transaction request) {

        StreamObserver<Null> observer = new StreamObserver<Null>() {

            @Override
            public void onNext(Null aNull) {

            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }
        };

        for (int i = 1; i <= nServers; ++i) {
            if (i == serverId) {
                continue;
            }
            JSONObject targetServer = (JSONObject) configJson.get(Integer.toString(i));

            String address = targetServer.getString("ip");
            int port = Integer.parseInt(targetServer.getString("port"));

            channel = ManagedChannelBuilder.forAddress(address, port).usePlaintext(true).build();
            blockingStub = BlockChainMinerGrpc.newBlockingStub(channel);
            asyncStub = BlockChainMinerGrpc.newStub(channel);
            asyncStub.pushTransaction(request, observer);
        }

        return true;
    }

    public boolean broadcast(Block request) {

        StreamObserver<Null> observer = new StreamObserver<Null>() {

            @Override
            public void onNext(Null aNull) {

            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }
        };

        for (int i = 1; i <= nServers; ++i) {
            JSONObject targetServer = (JSONObject) configJson.get(Integer.toString(i));

            String address = targetServer.getString("ip");
            int port = Integer.parseInt(targetServer.getString("port"));

            channel = ManagedChannelBuilder.forAddress(address, port).usePlaintext(true).build();
            blockingStub = BlockChainMinerGrpc.newBlockingStub(channel);
            asyncStub = BlockChainMinerGrpc.newStub(channel);
            try {
                asyncStub.pushBlock(JsonBlockString.newBuilder().setJson(JsonFormat.printer().print(request)).build(), observer);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return true;
    }

    // This is called when the server gets a transfer() or pushTransaction() GRPC call
    public boolean receive(Transaction request) {
        // Add semaphore?
        // TODO
        if (isValidTransaction(request) == false) {
            return false;
        }
        if (transactionRecords.contains(request.getUUID()) == false &&
                pendingTransactions.contains(request) == false) {
            //transfer(request);
            pendingTransactions.push(request);
            broadcast(request);
            changed = true;
            while(mining.isAlive()){Thread.currentThread().yield();}
            mining = new Mining(pendingTransactions, balances);
            changed = false;
            mining.start();
        }
        return true;
    }

    private boolean isValidTransaction(Transaction request) {
        if (request.getMiningFee() <= 0)
            return false;
        if (request.getValue() < request.getMiningFee())
            return false;
        return true;
    }


    public VerifyResponse verify(Transaction request) {
        VerifyResponse.Builder builder = VerifyResponse.newBuilder();
        Block recentBlock = blockChain.getLast();
        Block block = recentBlock;

        while (block != null && block.getBlockID() > 0) {
            if (block.getTransactionsList().contains(request)) {
                try {
                    VerifyResponse.Results flag;
                    if (recentBlock.getBlockID() - block.getBlockID() >= 6) {
                        flag = VerifyResponse.Results.SUCCEEDED;
                    } else {
                        flag = VerifyResponse.Results.PENDING;
                    }
                    return builder.setBlockHash(Hash.getHashString(JsonFormat.printer().print(block)))
                            .setResult(flag)
                            .build();
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                    return null;
                }
            }
            block = blockTree.get(block.getPrevHash());
        }
        return builder.setResult(VerifyResponse.Results.FAILED).build();
    }

    public GetHeightResponse getHeight() {
        try {
            Block recentBlock = blockChain.getLast();
            return GetHeightResponse.newBuilder().setHeight(recentBlock.getBlockID()).
                    setLeafHash(Hash.getHashString(JsonFormat.printer().print(recentBlock))).build();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public JsonBlockString getBlock(GetBlockRequest request) {
        if (blockTree.containsKey(request.getBlockHash()) == true) {
            Block block = blockTree.get(request.getBlockHash());
            String jsonString;
            try {
                jsonString = JsonFormat.printer().print(block);
            } catch (InvalidProtocolBufferException e) {
                //TODO
                //return null or a JSONBlockString with a null Json field?
                return null;
            }
            JsonBlockString blockString = JsonBlockString.newBuilder().setJson(jsonString).build();
            return blockString;
        }
        else {
            return null;
        }
    }

    //receive block from a another server
    public void pushBlock(JsonBlockString block) {
        String jsonString = block.getJson();
        LinkedList<Block> chain = new LinkedList<>();

        if (blockTree.containsKey(Hash.getHashString(block.getJson()))) {
            return;
        }

        try {
            Block.Builder builder = Block.newBuilder();
            JsonFormat.parser().merge(jsonString, builder);

            Block curBlock = builder.build();

            chain.add(curBlock);

            boolean flag = true;
            while (blockTree.containsKey(curBlock.getPrevHash()) == false) {
                if (curBlock.getPrevHash().equals(ZEROSTRING)) {
                    break;
                }
                curBlock = queryBlock(curBlock.getPrevHash());
                if (curBlock == null) {
                    flag = false;
                }
                chain.addFirst(curBlock);
            }

            if (flag == false) {
                return;
            }

            //Check whether this chain is valid, and switch
            // TODO
            checkAndSwitch(chain, blockChain, balances);


            /*
            //Add this new chain to Hashmap
            for (Block aBlock : chain) {
                blockTree.put(Hash.getHashString(aBlock.toString()), aBlock);
            }

            //Do we need to switch chain?
            if (chain.getLast().getBlockID() >= blockChain.getLast().getBlockID()) {
                if (Hash.getHashString(chain.getLast().toString()).compareTo(
                        Hash.getHashString(blockChain.getLast().toString())) < 0) {
                    switchChain(chain);
                }
            }
            */

        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }

    private Block queryBlock(String hash) {

        for (int i = 1; i <= nServers; ++i) {
            if (i == serverId) {
                continue;
            }
            JSONObject targetServer = (JSONObject) configJson.get(Integer.toString(i));

            String address = targetServer.getString("ip");
            int port = Integer.parseInt(targetServer.getString("port"));

            channel = ManagedChannelBuilder.forAddress(address, port).usePlaintext(true).build();
            blockingStub = BlockChainMinerGrpc.newBlockingStub(channel);
            asyncStub = BlockChainMinerGrpc.newStub(channel);
            GetBlockRequest request = GetBlockRequest.newBuilder().setBlockHash(hash).build();
            JsonBlockString blockString = blockingStub.getBlock(request);
            if (blockString != null) {
                Block.Builder builder = Block.newBuilder();
                try {
                    JsonFormat.parser().merge(blockString.getJson(), builder);
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                    return null;
                }
                return builder.build();
            }
        }

        return null;
    }

    private void checkAndSwitch(LinkedList<Block> newChain,
                                LinkedList<Block> oldChain,
                                DefaultHashMap<String, Integer> oldBalances) {
        while (oldChain.getLast() != newChain.getFirst()) {
            Block block = oldChain.getLast();
            List<Transaction> transactionList = block.getTransactionsList();
            Collections.reverse(transactionList);
            for (Transaction tranx : transactionList) {
                int fromBalance = oldBalances.get(tranx.getFromID());
                int toBalance = oldBalances.get(tranx.getToID());
                oldBalances.put(tranx.getFromID(), fromBalance + tranx.getValue());
                oldBalances.put(tranx.getToID(), toBalance - tranx.getValue() + tranx.getMiningFee());
            }
            oldChain.removeLast();
        }

        newChain.removeFirst();

        LinkedList<Block> tailoredChain = new LinkedList<>();

        for (Block newBlock : newChain) {
            boolean checkHash = Hash.checkHash(Hash.getHashString(newBlock.toString()));
            if (checkHash == false) {
                break;
            }

            boolean isBlockValid = true;

            //Do we need to check whether the block ID is valid?
            //TODO

            //Simulate the block, see if it has any invalid transactions
            List<Transaction> transactionList = newBlock.getTransactionsList();
            for (Transaction request : transactionList) {

                boolean isTranxValid = false;
                Transaction.Types type = request.getType();
                String fromId = request.getFromID();
                String toId = request.getToID();
                int value = request.getValue();
                int fee = request.getMiningFee();
                String UUID = request.getUUID();
                try {
                    semaphore.acquire();
                    if (type == Transaction.Types.TRANSFER && pattern.matcher(fromId).matches()
                            && pattern.matcher(toId).matches() && !fromId.equals(toId) && value >= 0
                            && fee >= 0) {
                        int fromBalance = oldBalances.get(fromId);
                        if (value <= fromBalance && value >= fee && fee > 0) {
                            int toBalance = oldBalances.get(toId);
                            oldBalances.put(fromId, fromBalance - value);
                            oldBalances.put(toId, toBalance + value - fee);
                            isTranxValid = true;

                            semaphore.release();
                        }
                    }
                    semaphore.release();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                if (isTranxValid == false) {
                    isBlockValid = false;
                    break;
                }

            }

            if (isBlockValid == true) {
                tailoredChain.add(newBlock);
            } else {
                break;
            }
        }

        //Add this new chain to Hashmap
        for (Block aBlock : tailoredChain) {
            blockTree.put(Hash.getHashString(aBlock.toString()), aBlock);
        }

        //Do we need to switch chain?
        if (tailoredChain.getLast().getBlockID() >= blockChain.getLast().getBlockID()) {
            if (Hash.getHashString(tailoredChain.getLast().toString()).compareTo(
                    Hash.getHashString(blockChain.getLast().toString())) < 0) {
                switchChain(tailoredChain);
                balances = oldBalances;
                oldChain.addAll(tailoredChain);
                blockChain = oldChain;
            }
        }
    }

    private void switchChain(LinkedList<Block> blockList) {

        // Only Need to modify transactionRecords and pendingTransactions?
        Block ancestorBlock = blockTree.get(blockList.getFirst().getPrevHash());
        while (blockChain.getLast() != ancestorBlock) {
            List<Transaction> transactionList = blockChain.getLast().getTransactionsList();
            blockChain.removeLast();
            for (Transaction request : transactionList) {
                transactionRecords.remove(request.getUUID());
                pendingTransactions.add(request);
            }
        }

        for (Block newBlock : blockList) {
            blockChain.add(newBlock);
            List<Transaction> transactionList = newBlock.getTransactionsList();
            for (Transaction request : transactionList) {
                transactionRecords.add(request.getUUID());
                pendingTransactions.remove(request);
            }
        }

        // Need to delete old thread, and create a new Mining Thread
        // TODO

        changed = true;
        while(mining.isAlive()){Thread.currentThread().yield();}
        mining = new Mining(pendingTransactions, balances);
        changed = false;
        mining.start();
    }

    private class DefaultHashMap<K,V> extends HashMap<K,V> {
        protected V defaultValue;
        public DefaultHashMap(V defaultValue) {
            this.defaultValue = defaultValue;
        }
        @Override
        public V get(Object k) {
            return containsKey(k) ? super.get(k) : defaultValue;
        }
    }

    private class Mining extends Thread {
        private int nonce = 0;
        private LinkedList<Transaction> pendingTransactions;
        private DefaultHashMap<String, Integer> balances;
        private Block block;
        public Mining(LinkedList<Transaction> pendingTransactions, DefaultHashMap<String, Integer> balances) {
            this.pendingTransactions = pendingTransactions;
            this.balances = balances;
        }
        public void run() {
            Block.Builder builder = Block.newBuilder().setBlockID(blockChain.size()+1);
            int sum = 0, value, fee, fromBalance, toBalance;
            String fromId, toId;
            for(Transaction transaction: pendingTransactions) {
                fromId = transaction.getFromID();
                toId = transaction.getToID();
                value = transaction.getValue();
                fee = transaction.getMiningFee();
                fromBalance = balances.get(fromId);
                if(!transactionRecords.contains(transaction.getUUID())
                        && transaction.getType()==Transaction.Types.TRANSFER
                        && pattern.matcher(fromId).matches()
                        && pattern.matcher(toId).matches()
                        && !fromId.equals(toId)
                        && fee >= 0 && value >= fee && fromBalance >= value) {
                    builder.addTransactions(transaction);
                    balances.put(fromId, fromBalance - value);
                    balances.put(toId, balances.get(toId) + value - fee);
                    sum++;
                }
                if(sum>=N)break;
            }
            while(!mined&&!changed) {
                block = builder.setNonce(Integer.toString(nonce)).build();
                try {
                    mined = Hash.checkHash(Hash.getHashString(JsonFormat.printer().print(block)));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                nonce++;
                if(nonce>=100000000)nonce=0;
            }
            if(mined&&!changed) {
                blockChain.add(block);
                broadcast(block);
                mined = false;
            }
        }
    }
}
