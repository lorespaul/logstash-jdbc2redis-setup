package it.lorenzodaneo.redismessagebroker.tests.mixed;

import com.lorenzodaneo.messagebroker.Message;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import net.minidev.json.annotate.JsonIgnore;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TestMessage implements Message {
    private String id;
    private String type;
    private String description;

    public Long getAggregateId(){
        return Math.abs((long)id.hashCode());
    }
}
