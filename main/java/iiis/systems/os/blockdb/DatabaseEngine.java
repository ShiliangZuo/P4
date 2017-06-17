package iiis.systems.os.blockdb;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.json.JSONObject;

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

    private DefaultHashMap<String, Integer> balances = new DefaultHashMap<>(1000);
    private int initBal = 1000;
    private String dataDir;
    private int serverId;
    private JSONObject configJson;
    private int nServers;

    private Semaphore semaphore = new Semaphore(1);

    private HashSet<String> transactionRecords = new HashSet<>();
    private LinkedList<Transaction> pendingTransactions = new LinkedList<>();

    private HashMap<String, Block> blockTree = new HashMap<>();
    private LinkedList<Block> blockChain = new LinkedList<>();

    //Use regular expression to match username
    final int userIdLength = 8;
    final String template = "[a-z0-9|-]{" + userIdLength + "}";
    Pattern pattern = Pattern.compile(template, Pattern.CASE_INSENSITIVE);

    private static ManagedChannel channel;
    private static BlockChainMinerGrpc.BlockChainMinerBlockingStub blockingStub;
    private static BlockChainMinerGrpc.BlockChainMinerStub asyncStub;

    private static String ZERO64 = "0000000000000000000000000000000000000000000000000000000000000000";
    private static String ZEROSTRING;

    DatabaseEngine(String dataDir, int id, JSONObject config) {
        this.dataDir = dataDir;
        this.serverId = id;
        this.configJson = config;
        this.nServers = config.getInt("nservers");
        for (int i = 0; i < 256; ++i)
            ZEROSTRING = ZERO64 + ZERO64 + ZERO64 + ZERO64;
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
        }
        return true;
    }


    public VerifyResponse verify(Transaction request) {
        VerifyResponse.Builder builder = VerifyResponse.newBuilder();
        Block recentBlock = blockChain.getLast();
        Block block = recentBlock;
        while (block != null && block.getBlockID() > 0)
        {
            if (block.getTransactionsList().contains(request))
                try {
                    VerifyResponse.Results flag;
                    if (recentBlock.getBlockID()-block.getBlockID()>=6) {
                        flag = VerifyResponse.Results.SUCCEEDED;
                    } else {
                        flag = VerifyResponse.Results.PENDING;
                    }
                    return builder.setBlockHash(Hash.getHashString(JsonFormat.printer().print(block)))
                            .setResult(flag)
                            .build();
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }
            block = blockTree.get(block.getPrevHash());
        }
        return builder.setResult(VerifyResponse.Results.FAILED).setBlockHash(null).build();
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
            JsonBlockString blockString = JsonBlockString.newBuilder().setJson(block.toString()).build();
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

        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }

    private Block queryBlock(String hash) {
        //TODO
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
            }
        }
    }

    private void switchChain(LinkedList<Block> blockList) {

        // 

        while (blockChain.getLast() != blockList.getFirst()) {
            reverseBlock(blockChain.getLast());
            blockChain.removeLast();
        }

        for (Block newBlock : blockList) {
            addToChain(newBlock);
        }

        // Need to delete old thread, and create a new Mining Thread
        // TODO
    }

    private void reverseBlock(Block Block) {
        //1. Maintain the correct balance
        //2. Add the transactions to pendingTransactions List
        //TODO
    }

    private void addToChain(Block block) {
        // Add a block to the chain
        // What should u do when you find out this block is invalid?
        //TODO
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
}

