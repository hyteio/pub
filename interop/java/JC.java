// Interop helper for interop/run.sh: a plain ActiveMQ JMS client (OpenWire),
// compiled against the jars of the ActiveMQ image under test.
// Usage: JC <brokerUrl> <queue> [user password]
// Prints one line per message (body, JMS headers, typed properties) until the
// queue is idle for 2s.
import org.apache.activemq.ActiveMQConnectionFactory;
import jakarta.jms.*;
import java.util.*;
public class JC {
  public static void main(String[] a) throws Exception {
    ActiveMQConnectionFactory f = new ActiveMQConnectionFactory(a[0]);
    if (a.length > 3) { f.setUserName(a[2]); f.setPassword(a[3]); }
    Connection c = f.createConnection(); c.start();
    Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
    MessageConsumer mc = s.createConsumer(s.createQueue(a[1]));
    Message m;
    while ((m = mc.receive(2000)) != null) {
      String body = m instanceof TextMessage ? "text:" + ((TextMessage)m).getText()
        : m instanceof BytesMessage ? "bytes:" + ((BytesMessage)m).getBodyLength() : m.getClass().getSimpleName();
      StringBuilder props = new StringBuilder();
      for (Enumeration<?> e = m.getPropertyNames(); e.hasMoreElements();) { String n = (String)e.nextElement(); Object v = m.getObjectProperty(n); props.append(n).append('=').append(v).append('(').append(v.getClass().getSimpleName()).append(") "); }
      System.out.println(body + " | prio=" + m.getJMSPriority() + " corr=" + m.getJMSCorrelationID() + " type=" + m.getJMSType() + " replyTo=" + m.getJMSReplyTo() + " persistent=" + (m.getJMSDeliveryMode()==DeliveryMode.PERSISTENT) + " exp=" + (m.getJMSExpiration()>0) + " | " + props);
    }
    c.close();
  }
}
