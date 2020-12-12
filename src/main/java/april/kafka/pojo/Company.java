package april.kafka.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author yanzx
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Company {

    private String name;

    private String address;
}
