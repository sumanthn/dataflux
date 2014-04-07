package dataflux.persister.util;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class PersistenceUtil {
	
	private static final DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyyMMddHHmmssSSS");
	private static final SimpleDateFormat a1df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	private static final MilliSecPadding msPad = new MilliSecPadding();

    private static Logger log = LoggerFactory.getLogger("PersistenceUtil");

	public static String reverseTimeStamp(String srcDt) {
		Date endDate;
		try {
			endDate = a1df.parse(srcDt);
			endDate.setTime(endDate.getTime() + msPad.getNextPadding());
			DateTime dt = new DateTime(endDate);
			return fmt.print(dt);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

    public static String reverseTimeStamp(long timeStamp) {
        //Date date;
        //try {
            //date = a1df.parse((new Date(timeStamp + msPad.getNextPadding())).toString());
            DateTime dt = new DateTime(timeStamp);
            return fmt.print(dt);
        /*} catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }*/
        //return null;
    }

    public static int getIntValue(String strVal, int defVal) {
        try {
            return Integer.parseInt(strVal);
        } catch (Exception exp) {
            log.error("Invalid int value : " + strVal);
        }
        return defVal;
    }

    public static long getLongValue(String strVal, int defVal) {
        try {
            return Long.parseLong(strVal);
        } catch (Exception exp) {
            log.error("Invalid long value : " + strVal);
        }
        return defVal;
    }

	private static class MilliSecPadding {
		/**
		 * Random number generator
		 */
		private Random rndm = new Random();
		private static final int TXN_STID = 100;
		private static final int TXN_ENID = 999;
		
		public long getNextPadding() {
			int random = TXN_STID;
			do {
				random = rndm.nextInt(TXN_ENID);
			} while((random < TXN_STID));
			return random;
		}
	}

	public static void main(String[] args) {
		System.out.println(reverseTimeStamp("2014-01-13 15:34:50"));
	}
}
