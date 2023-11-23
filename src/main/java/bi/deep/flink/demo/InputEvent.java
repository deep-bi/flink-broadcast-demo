package bi.deep.flink.demo;

public class InputEvent {

    public String uid;
    public String nameId;
    public String orgId;

    public InputEvent(String uid, String nameId, String orgId) {
        this.uid = uid;
        this.nameId = nameId;
        this.orgId = orgId;
    }
}
