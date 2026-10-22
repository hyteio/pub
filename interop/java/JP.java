// Interop helper for interop/run.sh: a plain ActiveMQ JMS client (OpenWire),
// compiled against the jars of the ActiveMQ Classic image under test.
// Usage: JP <brokerUrl> <queue> [user password]
// Sends a TextMessage with every JMS header and property type, a BytesMessage
// and a MapMessage. Add ?jms.useCompression=true to the URL to compress them.
import org.apache.activemq.ActiveMQConnectionFactory;
import jakarta.jms.*;
public class JP {
  public static void main(String[] a) throws Exception {
    ActiveMQConnectionFactory f = new ActiveMQConnectionFactory(a[0]);
    if (a.length > 3) { f.setUserName(a[2]); f.setPassword(a[3]); }
    Connection c = f.createConnection(); c.start();
    Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
    MessageProducer p = s.createProducer(s.createQueue(a[1]));
    TextMessage t = s.createTextMessage("from java ünïcödé 😀");
    t.setIntProperty("i", 42); t.setLongProperty("l", 1L<<40); t.setBooleanProperty("b", true);
    t.setDoubleProperty("d", 2.5); t.setByteProperty("y", (byte)-1); t.setShortProperty("s", (short)7);
    t.setFloatProperty("f", 1.25f); t.setStringProperty("str", "x");
    t.setJMSCorrelationID("jc-1"); t.setJMSType("jtype"); t.setJMSReplyTo(s.createTopic("jreplies"));
    t.setStringProperty("JMSXGroupID", "grp"); t.setIntProperty("JMSXGroupSeq", 3);
    p.setPriority(8); p.setTimeToLive(600000);
    p.send(t);
    BytesMessage b = s.createBytesMessage(); b.writeBytes(new byte[]{0,1,2,(byte)255}); p.send(b);
    MapMessage m = s.createMapMessage(); m.setInt("k", 1); p.send(m);
    c.close();
  }
}
