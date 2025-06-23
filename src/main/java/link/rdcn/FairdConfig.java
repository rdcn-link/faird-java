package link.rdcn;

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/17 13:37
 * @Modified By:
 */
public class FairdConfig {
    private String hostName;
    private String hostTitle;
    private String hostPosition;
    private String hostDomain;
    private int hostPort;
    private int catdbPort;

    // Getter & Setter
    public String getHostName() { return hostName; }
    public void setHostName(String hostName) { this.hostName = hostName; }

    public String getHostTitle() { return hostTitle; }
    public void setHostTitle(String hostTitle) { this.hostTitle = hostTitle; }

    public String getHostPosition() { return hostPosition; }
    public void setHostPosition(String hostPosition) { this.hostPosition = hostPosition; }

    public String getHostDomain() { return hostDomain; }
    public void setHostDomain(String hostDomain) { this.hostDomain = hostDomain; }

    public int getHostPort() { return hostPort; }
    public void setHostPort(int hostPort) { this.hostPort = hostPort; }

    public int getCatdbPort() { return catdbPort; }
    public void setCatdbPort(int catdbPort) { this.catdbPort = catdbPort; }
}
