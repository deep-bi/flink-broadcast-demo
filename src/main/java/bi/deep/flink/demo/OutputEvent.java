package bi.deep.flink.demo;

public class OutputEvent {

    public String uid;
    public String nameLabel;
    public String orgLabel;

    public OutputEvent(String uid, String nameLabel, String orgLabel) {
        this.uid = uid;
        this.nameLabel = nameLabel;
        this.orgLabel = orgLabel;
    }

    @Override
    public String toString() {
        return "OutputEvent{" +
                "uid='" + uid + '\'' +
                ", nameLabel='" + nameLabel + '\'' +
                ", orgLabel='" + orgLabel + '\'' +
                '}';
    }
}
