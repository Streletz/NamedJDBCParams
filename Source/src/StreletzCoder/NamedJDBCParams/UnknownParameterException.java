package StreletzCoder.NamedJDBCParams;

/**
 * Исключение "Неизвестный параметр"
 * 
 * @author Streletz, 2016, http://streletzcoder.ru/
 */
public class UnknownParameterException extends Exception {
    public UnknownParameterException(Throwable e) { 
        initCause(e); 
    } 

    public UnknownParameterException() {
        
    }
    
}
