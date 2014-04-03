package dataflux.datagenerator.types;

import java.util.ArrayList;
import java.util.List;

/**
 * A Web Txn template for data generation
 * Created by sumanthn
 */
public class WebTxn {

    private int id;
    private String opcode;
    private String url;
    private int bytes;
    //private String customattr;
    private List<String> customAttributes = new ArrayList<String>();



    public WebTxn(){}

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }


    public String getOpcode() {
        return opcode;
    }

    public void setOpcode(String opcode) {
        this.opcode = opcode;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }


    public int getBytes() {
        return bytes;
    }

    public void setBytes(int bytes) {
        this.bytes = bytes;
    }


    public List<String> getCustomAttributes() {
        return customAttributes;
    }

    public void setCustomAttributes(List<String> customAttributes) {
        this.customAttributes = customAttributes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WebTxn webTxn = (WebTxn) o;

        if (id != webTxn.id) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return id;
    }
}
