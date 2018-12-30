import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

/**
 * Author: baojianfeng
 * Date: 2018-09-30
 */
public class TimeUtil {

    /**
     * calculate age
     * @param val value
     * @return age
     */
    public static int calAge(String val) {
        String[] births = val.split("/");

        LocalDate birthday = LocalDate.of(Integer.parseInt(births[2]), Integer.parseInt(births[0]), Integer.parseInt(births[1]));
        LocalDate current = LocalDate.now();
        long age = ChronoUnit.YEARS.between(birthday, current);

        return (int) age;
    }
}
