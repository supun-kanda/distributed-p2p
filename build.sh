echo 'running on '$(pwd)

cd ./src/main/java
echo $(java -version)

echo removing class files
rm -f BootstrapServer.class Client.class ClientThread.class Neighbour.class Node\$1.class Node.class Server.class config/Constant.class

echo compiling files
javac BootstrapServer.java Node.java FtpServer.java

echo Done
