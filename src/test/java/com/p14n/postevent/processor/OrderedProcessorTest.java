package com.p14n.postevent.processor;

import com.p14n.postevent.data.Event;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.UUID;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class OrderedProcessorTest {

    private Connection mockConnection;
    private PreparedStatement mockPriorEventsStmt;
    private PreparedStatement mockHwmStmt;
    private PreparedStatement mockUpdateStmt;
    private ResultSet mockResultSet;
    private ResultSet mockHwmResultSet;
    private BiFunction<Connection, Event, Boolean> mockProcessor;
    private OrderedProcessor orderedProcessor;
    private Event testEvent;

    @BeforeEach
    public void setUp() throws SQLException {
        // Create mocks
        mockConnection = mock(Connection.class);
        mockPriorEventsStmt = mock(PreparedStatement.class);
        mockHwmStmt = mock(PreparedStatement.class);
        mockUpdateStmt = mock(PreparedStatement.class);
        mockResultSet = mock(ResultSet.class);
        mockHwmResultSet = mock(ResultSet.class);
        // Setup connection behavior
        when(mockConnection.prepareStatement(anyString())).thenAnswer(invocation -> {
            String sql = invocation.getArgument(0);
            if (sql.contains("SELECT COUNT(*)")) {
                return mockPriorEventsStmt;
            } else if (sql.contains("UPDATE")) {
                return mockUpdateStmt;
            } else if (sql.contains("SELECT hwm")) {
                return mockHwmStmt;
            }
            return mock(PreparedStatement.class);
        });

        // Setup statement behavior
        when(mockPriorEventsStmt.executeQuery()).thenReturn(mockResultSet);
        when(mockHwmStmt.executeQuery()).thenReturn(mockHwmResultSet);
        // Create test event
        testEvent = createTestEvent();

        // Create mock processor function
        mockProcessor = mock(BiFunction.class);

        // Create OrderedProcessor with mock processor
        orderedProcessor = new OrderedProcessor(mockProcessor);
    }

    @Test
    public void testProcessSuccessful() throws SQLException {
        // Setup mocks for successful processing
        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getInt(1)).thenReturn(0); // No prior unprocessed events
        when(mockUpdateStmt.executeUpdate()).thenReturn(1); // Update successful
        when(mockProcessor.apply(mockConnection, testEvent)).thenReturn(true); // Processing successful
        when(mockHwmResultSet.next()).thenReturn(true);
        when(mockHwmResultSet.getLong(1)).thenReturn(123L);
        // Execute
        boolean result = orderedProcessor.process(mockConnection, testEvent);

        // Verify
        assertTrue(result, "Process should return true for successful processing");

        // Verify transaction management
        verify(mockConnection).setAutoCommit(false);
        verify(mockConnection).commit();
        // Verify statements
        verify(mockPriorEventsStmt).setString(1, testEvent.subject());
        verify(mockPriorEventsStmt).setString(2, testEvent.topic());
        verify(mockPriorEventsStmt).setLong(3, testEvent.idn());
        verify(mockUpdateStmt).setLong(1, testEvent.idn());

        // Verify processor was called
        verify(mockProcessor).apply(mockConnection, testEvent);
    }

    @Test
    public void testProcessWithPriorUnprocessedEvents() throws SQLException {
        // Setup mocks for prior unprocessed events
        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getInt(1)).thenReturn(1); // One prior unprocessed event
        when(mockHwmResultSet.getInt(1)).thenReturn(1);

        // Execute
        boolean result = orderedProcessor.process(mockConnection, testEvent);

        // Verify
        assertFalse(result, "Process should return false when there are prior unprocessed events");

        // Verify transaction management
        verify(mockConnection).setAutoCommit(false);
        verify(mockConnection, never()).commit();

        // Verify statements
        verify(mockPriorEventsStmt).setString(1, testEvent.subject());
        verify(mockPriorEventsStmt).setString(2, testEvent.topic());
        verify(mockPriorEventsStmt).setLong(3, testEvent.idn());

        // Verify processor was not called
        verify(mockProcessor, never()).apply(any(), any());
    }

    @Test
    public void testProcessWithAlreadyProcessedEvent() throws SQLException {
        // Setup mocks for already processed event
        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getInt(1)).thenReturn(0); // No prior unprocessed events
        when(mockUpdateStmt.executeUpdate()).thenReturn(0); // Update failed (already processed)
        when(mockHwmResultSet.getInt(1)).thenReturn(1);

        // Execute
        boolean result = orderedProcessor.process(mockConnection, testEvent);

        // Verify
        assertFalse(result, "Process should return false when the event is already processed");

        // Verify transaction management
        verify(mockConnection).setAutoCommit(false);
        verify(mockConnection, never()).commit();
        // Verify processor was not called
        verify(mockProcessor, never()).apply(any(), any());
    }

    @Test
    public void testProcessWithProcessorFailure() throws SQLException {
        // Setup mocks for processor failure
        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getInt(1)).thenReturn(0); // No prior unprocessed events
        when(mockUpdateStmt.executeUpdate()).thenReturn(1); // Update successful
        when(mockProcessor.apply(mockConnection, testEvent)).thenReturn(false); // Processing failed
        when(mockHwmResultSet.next()).thenReturn(true);
        when(mockHwmResultSet.getLong(1)).thenReturn(123L);

        // Execute
        boolean result = orderedProcessor.process(mockConnection, testEvent);

        // Verify
        assertFalse(result, "Process should return false when processor returns false");

        // Verify transaction management
        verify(mockConnection).setAutoCommit(false);
        verify(mockConnection).rollback();
        verify(mockConnection, never()).commit();

        // Verify processor was called
        verify(mockProcessor).apply(mockConnection, testEvent);
    }

    @Test
    public void testProcessWithProcessorException() throws SQLException {
        // Setup mocks for processor exception
        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getInt(1)).thenReturn(0); // No prior unprocessed events
        when(mockUpdateStmt.executeUpdate()).thenReturn(1); // Update successful
        when(mockProcessor.apply(mockConnection, testEvent)).thenThrow(new RuntimeException("Test exception"));
        when(mockHwmResultSet.next()).thenReturn(true);
        when(mockHwmResultSet.getLong(1)).thenReturn(123L);

        // Execute
        boolean result = orderedProcessor.process(mockConnection, testEvent);

        // Verify
        assertFalse(result, "Process should return false when processor throws exception");

        // Verify transaction management
        verify(mockConnection).setAutoCommit(false);
        verify(mockConnection).rollback();
        verify(mockConnection, never()).commit();

        // Verify processor was called
        verify(mockProcessor).apply(mockConnection, testEvent);
    }

    @Test
    public void testProcessWithDatabaseException() throws SQLException {
        // Setup mocks for database exception
        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getInt(1)).thenReturn(0); // No prior unprocessed events
        when(mockUpdateStmt.executeUpdate()).thenThrow(new SQLException("Test SQL exception"));
        when(mockHwmResultSet.getInt(1)).thenReturn(1);

        // Execute
        boolean result = orderedProcessor.process(mockConnection, testEvent);

        // Verify
        assertFalse(result, "Process should return false when database throws exception");

        // Verify transaction management
        verify(mockConnection).setAutoCommit(false);
        verify(mockConnection).rollback();
        verify(mockConnection, never()).commit();

        // Verify processor was not called
        verify(mockProcessor, never()).apply(any(), any());
    }

    @Test
    public void testAutoCommitRestoration() throws SQLException {
        // Setup mocks for original autoCommit = true
        when(mockConnection.getAutoCommit()).thenReturn(true);
        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getInt(1)).thenReturn(0);
        when(mockUpdateStmt.executeUpdate()).thenReturn(1);
        when(mockProcessor.apply(mockConnection, testEvent)).thenReturn(true);
        when(mockHwmResultSet.getInt(1)).thenReturn(1);

        // Execute
        orderedProcessor.process(mockConnection, testEvent);

        // Setup mocks for original autoCommit = false
        reset(mockConnection, mockPriorEventsStmt, mockUpdateStmt, mockResultSet, mockProcessor);
        when(mockConnection.getAutoCommit()).thenReturn(false);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPriorEventsStmt);
        when(mockPriorEventsStmt.executeQuery()).thenReturn(mockResultSet);
        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getInt(1)).thenReturn(0);

        // Execute
        orderedProcessor.process(mockConnection, testEvent);

        // Verify autoCommit was restored
        verify(mockConnection).setAutoCommit(false);
    }

    @Test
    public void testProcessWithQueryException() throws SQLException {
        // Setup mocks for query exception
        when(mockPriorEventsStmt.executeQuery()).thenThrow(new SQLException("Test query exception"));
        when(mockHwmResultSet.getInt(1)).thenReturn(1);

        // Execute
        boolean result = orderedProcessor.process(mockConnection, testEvent);

        // Verify
        assertFalse(result, "Process should return false when query throws exception");

        // Verify transaction management
        verify(mockConnection).setAutoCommit(false);
        verify(mockConnection).rollback();
        verify(mockConnection, never()).commit();

        // Verify processor was not called
        verify(mockProcessor, never()).apply(any(), any());
    }

    private Event createTestEvent() {
        String id = UUID.randomUUID().toString();
        String source = "test-source";
        String type = "test-type";
        String contentType = "application/json";
        String dataSchema = "schema1";
        String subject = "test-subject";
        byte[] data = "{\"key\":\"value\"}".getBytes();
        Instant time = Instant.now();
        long idn = 123L;

        return Event.create(
                id,
                source,
                type,
                contentType,
                dataSchema,
                subject,
                data,
                time,
                idn,
                "topic");
    }
}