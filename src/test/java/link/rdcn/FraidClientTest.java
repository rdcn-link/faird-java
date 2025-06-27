package link.rdcn;

import link.rdcn.client.FairdClient;

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/26 18:50
 * @Modified By:
 */
public class FraidClientTest {
    public static void main(String[] args) {
        FairdClient dc = FairdClient.connect("dacp://0.0.0.0:3101");
        System.out.println(dc.getDataSetMetaData("dffd"));
    }

}
