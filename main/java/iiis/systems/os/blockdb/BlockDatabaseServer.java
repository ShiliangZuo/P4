package iiis.systems.os.blockdb;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;

public class BlockDatabaseServer {
    private Server server;

    private void start(String address, int port) throws IOException {
        server = NettyServerBuilder.forAddress(new InetSocketAddress(address, port))
                .addService(new BlockDatabaseImpl())
                .build()
                .start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                BlockDatabaseServer.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static void main(String[] args) throws IOException, JSONException, InterruptedException {

        System.out.println(args.length);
        for (int i = 0 ; i < args.length; ++i) {
            System.out.println(args[i]);
        }

        int id = 0;

        if (args.length == 0 || args[0].substring(0,5).equals("--id=")) {
            id = Integer.parseInt(args[0].substring(5));
        }

        System.out.println("Server about to start...!!");

        JSONObject config = Util.readJsonFile("config.json");

        JSONObject thisServer = (JSONObject)config.get("1");

        String address = thisServer.getString("ip");
        int port = Integer.parseInt(thisServer.getString("port"));
        String dataDir = thisServer.getString("dataDir");

        DatabaseEngine.setup(dataDir, id, config);

        final BlockDatabaseServer server = new BlockDatabaseServer();
        server.start(address, port);
        server.blockUntilShutdown();
    }

    static class BlockDatabaseImpl extends BlockChainMinerGrpc.BlockChainMinerImplBase {
        private final DatabaseEngine dbEngine = DatabaseEngine.getInstance();

        @Override
        public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
            int value = dbEngine.get(request.getUserID());
            GetResponse response = GetResponse.newBuilder().setValue(value).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void transfer(Transaction request, StreamObserver<BooleanResponse> responseObserver) {
            //boolean transferSuccess = dbEngine.transfer(request);
            //boolean broadcastSuccess = dbEngine.broadcast(request);
            //boolean success = transferSuccess && broadcastSuccess;

            BooleanResponse response = BooleanResponse.newBuilder().setSuccess(true).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void verify(Transaction request, StreamObserver<VerifyResponse> responseObserver) {
            VerifyResponse response = dbEngine.verify(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void getBlock(GetBlockRequest request, StreamObserver<JsonBlockString> responseObserver) {
            JsonBlockString blockString = dbEngine.getBlock(request);
            responseObserver.onNext(blockString);
            responseObserver.onCompleted();
        }

        @Override
        public void getHeight(Null aNull, StreamObserver<GetHeightResponse> responseObserver) {
            GetHeightResponse response = dbEngine.getHeight();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }


        @Override
        public void pushTransaction(Transaction request, StreamObserver<Null> responseObserver) {
            dbEngine.receive(request);
            Null response = Null.newBuilder().build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void pushBlock(JsonBlockString block, StreamObserver<Null> responseObserver) {
            dbEngine.pushBlock(block);
            Null response = Null.newBuilder().build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

    }
}
