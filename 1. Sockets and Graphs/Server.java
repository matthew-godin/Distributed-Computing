import java.io.*;
import java.nio.charset.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

class Server {
	private static byte[] bytes;
	private static int currentInteger0,
					   currentInteger1;
	private static HashMap<Integer, InitializedInt> vertexDegrees0,
													vertexDegrees1;

	private static void assignKeyValue0() {
		InitializedInt vertexDegree
			= vertexDegrees0.get(currentInteger0);
		if (vertexDegree == null) {
			vertexDegrees0.put(currentInteger0,
				new InitializedInt());
		} else {
			vertexDegree.increment();
		}
		currentInteger0 = 0;
	}

	private static void assignKeyValue1() {
		InitializedInt vertexDegree
			= vertexDegrees1.get(currentInteger1);
		if (vertexDegree == null) {
			vertexDegrees1.put(currentInteger1,
				new InitializedInt());
		} else {
			vertexDegree.increment();
		}
		currentInteger1 = 0;
	}

	public static void main(String args[]) throws Exception {
		if (args.length != 1) {
			System.out.println("usage: java Server port");
			System.exit(-1);
		}
		int port = Integer.parseInt(args[0]);
		ServerSocket ssock = new ServerSocket(port);
		System.out.println("listening on port " + port);
		byte startOfLineBreak = (byte)System.lineSeparator().charAt(0);
		while(true) {
			try {
				/*
				  YOUR CODE GOES HERE
				  - accept a connection from the server socket
				  - add an inner loop to read requests from this connection
				    repeatedly (client may reuse the connection for multiple
				    requests)
				  - for each request, compute an output and send a response
				  - each message has a 4-byte header followed by a payload
				  - the header is the length of the payload
				    (signed, two's complement, big-endian)
				  - the payload is a string (UTF-8)
				  - the inner loop ends when the client closes the connection
				*/
				Socket sock = ssock.accept();
				while (!sock.isClosed()) {
					long startTime = System.currentTimeMillis();
					DataInputStream din = new DataInputStream(sock.getInputStream());
					int reqDataLen = din.readInt();
					bytes = new byte[reqDataLen];
					din.readFully(bytes);
					int bytesHalfMark = reqDataLen / 2;
					for (;; ++bytesHalfMark) {
						if (bytes[bytesHalfMark] == startOfLineBreak) {
							bytesHalfMark += System.lineSeparator().length();
							break;
						}
					}
					vertexDegrees0 = new HashMap<Integer, InitializedInt>();
					vertexDegrees1 = new HashMap<Integer, InitializedInt>();
					final int effectiveHalfMark = bytesHalfMark;
					startTime = System.currentTimeMillis();
					Thread thread0 = new Thread() {
						public void run() {
							currentInteger0 = 0;
							for (int i = 0; i < effectiveHalfMark; ++i) {
								if (bytes[i] == startOfLineBreak) {
									i += System.lineSeparator().length() - 1;
									assignKeyValue0();
								} else if (bytes[i] == ' ') {
									assignKeyValue0();
								} else {
									currentInteger0
										= 10 * currentInteger0
										+ (bytes[i] - '0');
								}
							}
						}
					};
					Thread thread1 = new Thread() {
						public void run() {
							currentInteger1 = 0;
							for (int i = effectiveHalfMark; i < reqDataLen; ++i) {
								if (bytes[i] == startOfLineBreak) {
									i += System.lineSeparator().length() - 1;
									assignKeyValue1();
								} else if (bytes[i] == ' ') {
									assignKeyValue1();
								} else {
									currentInteger1
										= 10 * currentInteger1
										+ (bytes[i] - '0');
								}
							}
						}
					};
					thread0.start();
					thread1.start();
					thread0.join();
					thread1.join();
					StringBuilder outputBuilder = new StringBuilder();
					for (Map.Entry<Integer, InitializedInt> entry
						: vertexDegrees0.entrySet()) {
						outputBuilder.append(entry.getKey());
						outputBuilder.append(" ");
						InitializedInt vertexDegree
							= vertexDegrees1.get(entry.getKey());
						if (vertexDegree == null) {
							outputBuilder.append(entry.getValue().get());
						} else {
							outputBuilder.append(entry.getValue().get()
								+ vertexDegree.get());
						}
						outputBuilder.append(System.lineSeparator());
					}
					for (Map.Entry<Integer, InitializedInt> entry
						: vertexDegrees1.entrySet()) {
						InitializedInt vertexDegree
							= vertexDegrees0.get(entry.getKey());
						if (vertexDegree == null) {
							outputBuilder.append(entry.getKey());
							outputBuilder.append(" ");
							outputBuilder.append(entry.getValue().get());
							outputBuilder.append(System.lineSeparator());
						}
					}
					DataOutputStream dout = new DataOutputStream(sock.getOutputStream());
					bytes = outputBuilder.toString().getBytes("UTF-8");
					dout.writeInt(bytes.length);
					dout.write(bytes);
					dout.flush();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
