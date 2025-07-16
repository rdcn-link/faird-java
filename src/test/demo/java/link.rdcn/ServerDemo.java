/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/15 10:57
 * @Modified By:
 */
package link.rdcn;

import link.rdcn.TestProvider;
import link.rdcn.client.FairdClient;
import link.rdcn.provider.DataProvider;
import link.rdcn.server.FairdServer;

import javax.xml.crypto.Data;

public class ServerDemo {
    public static void main(String[] args) {
        TestProvider provider = new TestProvider();
        FairdServer server = new FairdServer(provider.dataProvider(),provider.authProvider(),provider.getResourcePath(""));
        server.start();
    }
}
