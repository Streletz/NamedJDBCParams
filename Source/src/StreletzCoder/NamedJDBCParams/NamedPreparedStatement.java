package StreletzCoder.NamedJDBCParams;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author Streletz, 2016, http://streletzcoder.ru/
 */
public class NamedPreparedStatement {

    private static final String REGEX = "(:[a-z]+)|(:[A-Z]+)";
    private static final String REPLACEMENT = "?";
    private final PreparedStatement pstmt;
    private final Connection con;    
    private final HashMap<String, Integer> paramsMap;

    /**
     * Конструктор
     *
     * @param connection Соединение с базой данных
     * @param sql Параметрический SQL запрос
     * @throws SQLException
     */
    public NamedPreparedStatement(Connection connection, String sql) throws SQLException {
        con = connection;       
        pstmt = this.con.prepareStatement(getConvertedSql(sql));
        paramsMap = this.getParamsMap(sql);
    }

    /**
     * Возвращает объект PreparedStatement
     *
     * @return
     */
    public PreparedStatement getPreparedStatement() {
        return pstmt;
    }

    /*Методы-обёртки для часто используемого функционала PreparedStatement чтобы избежать лишней цепочки методов*/
    /**
     * Выполнение запроса на выборку
     *
     * @return Результаты запросы
     * @throws SQLException
     */
    public ResultSet executeQuery() throws SQLException {
        return pstmt.executeQuery();
    }

    /**
     * Выполнение запроса не предусматривающего возврат данных
     *
     * @return Результат выполнения запроса
     * @throws SQLException
     */
    public boolean execute() throws SQLException {
        return pstmt.execute();
    }

    /**
     * Преобразует исходный SQL запрос в SQL запрос с анонимными параметрами для
     * работы объекта PreparedStatement
     *
     * @param sql Исходный SQL запрос
     * @return Преобразованный SQL запрос
     */
    private String getConvertedSql(String sql) {
        Matcher matcher = getMatcher(sql);
        return matcher.replaceAll(REPLACEMENT);
    }

    /**
     * Поиск параметров в запросе (общие механизмы)
     *
     * @param sql
     * @return Результат поиска в виде объекта Matcher
     */
    private Matcher getMatcher(String sql) {
        Pattern pattern = Pattern.compile(REGEX, 0);
        Matcher matcher = pattern.matcher(sql);
        return matcher;
    }

    /**
     * Формирует коллекцию из праметров запроса и их индексов
     *
     * @param sql SQL запрос
     * @return
     */
    private HashMap<String, Integer> getParamsMap(String sql) {
        Matcher matcher = getMatcher(sql);
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        int i = 0;
        while (matcher.find()) {
            i++;
            map.put(matcher.group().substring(1), i);
        }
        return map;
    }

    /**
     * Получает индекс параметра по его имени
     *
     * @param paramName Параметр
     *
     */
    private int getParamIndex(String paramName) throws UnknownParameterException {
        try {
            return paramsMap.get(paramName);
        } catch (NullPointerException ex) {
            throw new UnknownParameterException();
        }
    }

    /*СЕТТЕРЫ ДЛЯ ПАРАМЕТРОВ*/
    /**
     * Устанавливает значение параметра запроса типа массив (Array)
     *
     * @param paramName Параметр
     * @param value Значение
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setArray(String paramName, Array value) throws SQLException, UnknownParameterException {
        pstmt.setArray(getParamIndex(paramName), value);
    }

    /**
     * Устанавливает значение параметра запроса из потока символьных данных в
     * формате ASCII (InputStream)
     *
     * @param paramName Параметр
     * @param stream Поток
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setAsciiStream(String paramName, InputStream stream) throws SQLException, UnknownParameterException {
        pstmt.setAsciiStream(getParamIndex(paramName), stream);
    }

    /**
     * Устанавливает значение параметра из типа потока символьных данных в
     * формате ASCII (InputStream)
     *
     * @param paramName Параметр
     * @param stream Поток
     * @param streamLength Размер блока для чтения
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setAsciiStream(String paramName, InputStream stream, long streamLength) throws SQLException, UnknownParameterException {
        pstmt.setAsciiStream(getParamIndex(paramName), stream, streamLength);
    }

    /**
     * Устанавливает значение параметра запроса из потока символьных данных в
     * формате ASCII (InputStream)
     *
     * @param paramName Параметр
     * @param stream Поток
     * @param streamLength Размер блока для чтения
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setAsciiStream(String paramName, InputStream stream, int streamLength) throws SQLException, UnknownParameterException {
        pstmt.setAsciiStream(getParamIndex(paramName), stream, streamLength);
    }

    /**
     * Устанавливает значение параметра запроса типа число с плавающей точкой
     * (BigDecimal)
     *
     * @param paramName Параметр
     * @param value Значение
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setBigDecimal(String paramName, BigDecimal value) throws SQLException, UnknownParameterException {
        pstmt.setBigDecimal(getParamIndex(paramName), value);
    }

    /**
     * Устанавливает значение параметра запроса из потока двоичных данных
     * (InputStream)
     *
     * @param paramName Параметр
     * @param stream Поток
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setBinaryStream(String paramName, InputStream stream) throws SQLException, UnknownParameterException {
        pstmt.setBinaryStream(getParamIndex(paramName), stream);
    }

    /**
     * Устанавливает значение параметра запроса из потока двоичных данных
     * (InputStream)
     *
     * @param paramName Параметр
     * @param stream Поток
     * @param streamLength Размер блока для чтения
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setBinaryStream(String paramName, InputStream stream, long streamLength) throws SQLException, UnknownParameterException {
        pstmt.setBinaryStream(getParamIndex(paramName), stream, streamLength);
    }

    /**
     * Устанавливает значение параметра запроса из потока двоичных данных
     * (InputStream)
     *
     * @param paramName Параметр
     * @param stream Поток
     * @param streamLength Размер блока для чтения
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setBinaryStream(String paramName, InputStream stream, int streamLength) throws SQLException, UnknownParameterException {
        pstmt.setBinaryStream(getParamIndex(paramName), stream, streamLength);
    }

    /**
     * Устанавливает значение параметра запроса типа BLOB
     *
     * @param paramName Параметр
     * @param value Значение
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setBlob(String paramName, Blob value) throws SQLException, UnknownParameterException {
        pstmt.setBlob(getParamIndex(paramName), value);
    }

    /**
     * Устанавливает значение параметра запроса типа BLOB из потока
     *
     * @param paramName Параметр
     * @param stream Поток
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setBlob(String paramName, InputStream stream) throws SQLException, UnknownParameterException {
        pstmt.setBlob(getParamIndex(paramName), stream);
    }

    /**
     * Устанавливает значение параметра запроса типа BLOB из потока
     *
     * @param paramName Параметр
     * @param value Поток
     * @param streamLength Размер блока для чтения
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setBlob(String paramName, InputStream value, long streamLength) throws SQLException, UnknownParameterException {
        pstmt.setBlob(getParamIndex(paramName), value, streamLength);
    }

    /**
     * Устанавливает значение параметра запроса логического типа (Boolean)
     *
     * @param paramName Параметр
     * @param value Значение
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setBoolean(String paramName, Boolean value) throws SQLException, UnknownParameterException {
        pstmt.setBoolean(getParamIndex(paramName), value);
    }

    /**
     * Устанавливает значение параметра запроса типа байт (byte)
     *
     * @param paramName Параметр
     * @param value Значение
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setByte(String paramName, byte value) throws SQLException, UnknownParameterException {
        pstmt.setByte(getParamIndex(paramName), value);
    }

    /**
     * Устанавливает значение параметра запроса типа массив байт (byte[])
     *
     * @param paramName Параметр
     * @param value Значение
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setBytes(String paramName, byte value[]) throws SQLException, UnknownParameterException {
        pstmt.setBytes(getParamIndex(paramName), value);
    }

    /**
     * Устанавливает значение параметра запроса из файла в формате UNICODE
     *
     * @param paramName Параметр
     * @param reader Поток
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setCharacterStream(String paramName, Reader reader) throws SQLException, UnknownParameterException {
        pstmt.setCharacterStream(getParamIndex(paramName), reader);
    }

    /**
     * Устанавливает значение параметра из файла в формате UNICODE
     *
     * @param paramName Параметр
     * @param reader Поток
     * @param blockLength Размер блока для чтения
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setCharacterStream(String paramName, Reader reader, long blockLength) throws SQLException, UnknownParameterException {
        pstmt.setCharacterStream(getParamIndex(paramName), reader, blockLength);
    }

    /**
     * Устанавливает значение параметра запроса из файла в формате UNICODE
     *
     * @param paramName Параметр
     * @param reader Поток
     * @param blockLength Размер блока для чтения
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setCharacterStream(String paramName, Reader reader, int blockLength) throws SQLException, UnknownParameterException {
        pstmt.setCharacterStream(getParamIndex(paramName), reader, blockLength);
    }

    /**
     * Устанавливает значение параметра запроса типа CLOB
     *
     * @param paramName Параметр
     * @param value Значение
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setClob(String paramName, Clob value) throws SQLException, UnknownParameterException {
        pstmt.setClob(getParamIndex(paramName), value);
    }

    /**
     * Устанавливает значение параметра запроса типа CLOB из файла
     *
     * @param paramName Параметр
     * @param reader Поток
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setClob(String paramName, Reader reader) throws SQLException, UnknownParameterException {
        pstmt.setClob(getParamIndex(paramName), reader);
    }

    /**
     * Устанавливает значение параметра запроса типа CLOB из файла
     *
     * @param paramName Параметр
     * @param reader Поток
     * @param blockLength Размер блока для чтения
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setClob(String paramName, Reader reader, long blockLength) throws SQLException, UnknownParameterException {
        pstmt.setClob(getParamIndex(paramName), reader, blockLength);
    }

    /**
     * Устанавливает значение параметра запроса типа дата (Date)
     *
     * @param paramName Параметр
     * @param value Значение
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setDate(String paramName, Date value) throws SQLException, UnknownParameterException {
        pstmt.setDate(getParamIndex(paramName), value);
    }

    /**
     * Устанавливает значение параметра запроса типа дата (Date)
     *
     * @param paramName Параметр
     * @param value Значение
     * @param calendar Календарь
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setDate(String paramName, Date value, Calendar calendar) throws SQLException, UnknownParameterException {
        pstmt.setDate(getParamIndex(paramName), value, calendar);
    }

    /**
     * Устанавливает значение параметра запроса типа число с плавающей точкой
     * (double)
     *
     * @param paramName Параметр
     * @param value Значение
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setDouble(String paramName, double value) throws SQLException, UnknownParameterException {

        pstmt.setDouble(getParamIndex(paramName), value);
    }

    /**
     * Устанавливает значение параметра запроса типа число с плавающей точкой
     * (float)
     *
     * @param paramName Параметр
     * @param value Значение
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setFloat(String paramName, float value) throws SQLException, UnknownParameterException {
        pstmt.setFloat(getParamIndex(paramName), value);
    }

    /**
     * Устанавливает значение параметра запроса типа целое число (int)
     *
     * @param paramName Параметр
     * @param value Значение
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setInt(String paramName, int value) throws SQLException, UnknownParameterException {
        pstmt.setInt(getParamIndex(paramName), value);
    }

    /**
     * Устанавливает значение параметра запроса типа целое число (long)
     *
     * @param paramName Параметр
     * @param value Значение
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setLong(String paramName, long value) throws SQLException, UnknownParameterException {
        pstmt.setLong(getParamIndex(paramName), value);
    }

    /**
     * Устанавливает значение параметра запроса из потока символьных данных в
     * формате UNICODE с преобразованием в кодировку БД
     *
     * @param paramName Параметр
     * @param reader Поток
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setCNharacterStream(String paramName, Reader reader) throws SQLException, UnknownParameterException {
        pstmt.setNCharacterStream(getParamIndex(paramName), reader);
    }

    /**
     * Устанавливает значение параметра из файла в формате UNICODE с
     * преобразованием в кодировку БД
     *
     * @param paramName Параметр
     * @param reader Поток
     * @param blockLength Размер блока для чтения
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setNCharacterStream(String paramName, Reader reader, long blockLength) throws SQLException, UnknownParameterException {
        pstmt.setNCharacterStream(getParamIndex(paramName), reader, blockLength);
    }

    /**
     * Устанавливает значение параметра запроса типа NCLOB
     *
     * @param paramName Параметр
     * @param value Значение
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setNClob(String paramName, NClob value) throws SQLException, UnknownParameterException {
        pstmt.setNClob(getParamIndex(paramName), value);
    }

    /**
     * Устанавливает значение параметра запроса типа NCLOB из потока
     *
     * @param paramName Параметр
     * @param reader Поток
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setNClob(String paramName, Reader reader) throws SQLException, UnknownParameterException {
        pstmt.setNClob(getParamIndex(paramName), reader);
    }

    /**
     * Устанавливает значение параметра запроса типа NCLOB из потока
     *
     * @param paramName Параметр
     * @param reader Поток
     * @param blockLength Размер блока для чтения
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setNClob(String paramName, Reader reader, long blockLength) throws SQLException, UnknownParameterException {
        pstmt.setNClob(getParamIndex(paramName), reader, blockLength);
    }

    /**
     * Устанавливает значение параметра запроса типа строка (String) с
     * преобразованием в кодироку БД
     *
     * @param paramName Параметр
     * @param value Значение
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setNString(String paramName, String value) throws SQLException, UnknownParameterException {
        pstmt.setNString(getParamIndex(paramName), value);
    }

    /**
     * Устанавливает значение параметра запроса равное NULL
     *
     * @param paramName Параметр
     * @param sqlType Тип данных БД
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setNull(String paramName, int sqlType) throws SQLException, UnknownParameterException {
        pstmt.setNull(getParamIndex(paramName), sqlType);
    }

    /**
     * Устанавливает значение параметра запроса равное NULL
     *
     * @param paramName Параметр
     * @param sqlType Тип данных БД
     * @param typeName Имя типа данных БД
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setNull(String paramName, int sqlType, String typeName) throws SQLException, UnknownParameterException {
        pstmt.setNull(getParamIndex(paramName), sqlType, typeName);
    }

    /**
     * Устанавливает значение параметра запроса типа Object
     *
     * @param paramName Параметр
     * @param value Значение
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setObject(String paramName, Object value) throws SQLException, UnknownParameterException {
        pstmt.setObject(getParamIndex(paramName), value);
    }

    /**
     * Устанавливает значение параметра запроса типа Object
     *
     * @param paramName Параметр
     * @param value Значение
     * @param targetSQLType Тип данных поля БД
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setObject(String paramName, Object value, SQLType targetSQLType) throws SQLException, UnknownParameterException {
        pstmt.setObject(getParamIndex(paramName), targetSQLType);
    }

    /**
     * Устанавливает значение параметра запроса типа Object
     *
     * @param paramName Параметр
     * @param value Значение
     * @param targetSQLType Тип данных поля БД
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setObject(String paramName, Object value, int targetSQLType) throws SQLException, UnknownParameterException {
        pstmt.setObject(getParamIndex(paramName), targetSQLType);
    }

    /**
     * Устанавливает значение параметра запроса типа Object
     *
     * @param paramName Параметр
     * @param value Значение
     * @param targetSQLType Тип данных поля БД
     * @param sizeOrLength Размер поля
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setObject(String paramName, Object value, SQLType targetSQLType, int sizeOrLength) throws SQLException, UnknownParameterException {
        pstmt.setObject(getParamIndex(paramName), targetSQLType, sizeOrLength);
    }

    /**
     * Устанавливает значение параметра запроса типа Object
     *
     * @param paramName Параметр
     * @param value Значение
     * @param targetSQLType Тип данных поля БД
     * @param sizeOrLength Размер поля
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setObject(String paramName, Object value, int targetSQLType, int sizeOrLength) throws SQLException, UnknownParameterException {
        pstmt.setObject(getParamIndex(paramName), targetSQLType, sizeOrLength); 
    }
    
    /**
     * Устанавливает значение параметра запроса структурированного типа 
     *
     * @param paramName Параметр
     * @param value Значение
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setRef(String paramName, Ref value) throws SQLException, UnknownParameterException {
        pstmt.setRef(getParamIndex(paramName),value); 
    }
    
    /**
     * Устанавливает значение параметра запроса типа RowId
     *
     * @param paramName Параметр
     * @param value Значение
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setRowId(String paramName, RowId value) throws SQLException, UnknownParameterException {
        pstmt.setRowId(getParamIndex(paramName),value); 
    }
    
    /**
     * Устанавливает значение параметра запроса типа SQLXML
     *
     * @param paramName Параметр
     * @param value Значение
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setSQLXML(String paramName, SQLXML value) throws SQLException, UnknownParameterException {
        pstmt.setSQLXML(getParamIndex(paramName), value);
    }
    
    /**
     * Устанавливает значение параметра запроса типа целое число (short)
     *
     * @param paramName Параметр
     * @param value Значение
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setShort(String paramName, short value) throws SQLException, UnknownParameterException {
        pstmt.setShort(getParamIndex(paramName), value);
    }

    /**
     * Устанавливает значение параметра запроса типа строка (String)
     *
     * @param paramName Параметр
     * @param value Значение
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setString(String paramName, String value) throws SQLException, UnknownParameterException {
        pstmt.setString(getParamIndex(paramName), value);
    }
    /**
     * Устанавливает значение параметра запроса типа время (Time)
     *
     * @param paramName Параметр
     * @param value Значение
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setTime(String paramName, Time value) throws SQLException, UnknownParameterException {
        pstmt.setTime(getParamIndex(paramName), value);
    }
    /**
     * Устанавливает значение параметра запроса типа время (Time)
     *
     * @param paramName Параметр
     * @param value Значение
     * @param calendar Календарь
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setTime(String paramName, Time value,Calendar calendar) throws SQLException, UnknownParameterException {
        pstmt.setTime(getParamIndex(paramName), value, calendar);
    }
     /**
     * Устанавливает значение параметра запроса типа Timestamp
     *
     * @param paramName Параметр
     * @param value Значение
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setTimeStamp(String paramName, Timestamp value) throws SQLException, UnknownParameterException {
        pstmt.setTimestamp(getParamIndex(paramName), value);
    }
    /**
     * Устанавливает значение параметра запроса типа Timestamp
     *
     * @param paramName Параметр
     * @param value Значение
     * @param calendar Календарь
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setTimeStamp(String paramName, Timestamp value,Calendar calendar) throws SQLException, UnknownParameterException {
        pstmt.setTimestamp(getParamIndex(paramName), value, calendar); 
    }
    /**
     * Устанавливает значение параметра запроса типа URL
     *
     * @param paramName Параметр
     * @param value Значение
     * @throws SQLException
     * @throws StreletzCoder.NamedJDBCParams.UnknownParameterException
     */
    public void setURL(String paramName, URL value) throws SQLException, UnknownParameterException {
        pstmt.setURL(getParamIndex(paramName), value);
    }

}
