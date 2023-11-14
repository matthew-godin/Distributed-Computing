import java.util.ArrayList;
import java.util.List;

import org.mindrot.jbcrypt.BCrypt;

public class FEBcryptServiceHandler extends BcryptServiceHandler {
	List<BENode> BENodes = new ArrayList<BENode>();

	public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException
	{
		return super.hashPassword(password, logRounds);
	}

	public List<Boolean> checkPassword(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException
	{
		return super.checkPassword(password, hash);
	}

	public synchronized void registerBENode(String host, int port) {
		try {
			BENodes.add(new BENode(host, port));
		} catch (Exception e) { }
	}
}
