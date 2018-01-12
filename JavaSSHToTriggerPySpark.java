import java.io.InputStream;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;


public class JavaSSHToTriggerPySpark {



	/**
	 * JSch Example Tutorial
	 * Java SSH Connection to trigger pyspark algorithm outside hadoop cluster.
	 */
	public static void main(String[] args) {
		triggerPySparkAlgorithm();

	}

	private static void triggerPySparkAlgorithm() {
		String host="SSH_HOST";
		String user="your username";
		String password="your password";
		String command1="spark-submit sample.py"; //sample.py is the pyspark algorithm which runs in hadoop cluster.
		try{
			long start=System.currentTimeMillis();
			java.util.Properties config = new java.util.Properties(); 
			config.put("StrictHostKeyChecking", "no");
			JSch jsch = new JSch();
			Session session=jsch.getSession(user, host, 22);
			session.setPassword(password);
			session.setConfig(config);
			session.connect();
			System.out.println("Connected Using SSH = "+(System.currentTimeMillis()-start));

			Channel channel=session.openChannel("exec");
			((ChannelExec)channel).setCommand(command1);
			channel.setInputStream(null);
			((ChannelExec)channel).setErrStream(System.err);

			InputStream in=channel.getInputStream();
			channel.connect();
			byte[] tmp=new byte[1024];
			while(true){
				while(in.available()>0){
					int i=in.read(tmp, 0, 1024);
					if(i<0)break;
					System.out.print(new String(tmp, 0, i));
				}
				if(channel.isClosed()){
					System.out.println("exit-status: "+channel.getExitStatus());
					break;
				}
				try{Thread.sleep(1000);}catch(Exception ee){}
			}
			channel.disconnect();
			session.disconnect();
			System.out.println("DONE :: TIME ="+(System.currentTimeMillis()-start));
		}catch(Exception e){
			e.printStackTrace();
		}
	}



}
