package com.p14n.postevent.catchup;

import com.p14n.postevent.data.Event;
import java.util.List;

/**
 * Interface for fetching events from a data store.
 * Provides methods to retrieve events within a specified range and get the
 * latest message ID
 * for a given topic. This interface is used to implement catch-up functionality
 * for clients
 * that have missed events or need to synchronize their state.
 *
 * <p>
 * Implementations of this interface should:
 * </p>
 * <ul>
 * <li>Handle event retrieval from persistent storage</li>
 * <li>Ensure events are returned in order by sequence number (idn)</li>
 * <li>Apply appropriate validation for input parameters</li>
 * <li>Handle topic-specific event retrieval</li>
 * </ul>
 */
public interface CatchupServerInterface {

    /**
     * Fetches events from a data store within a specified range.
     * Returns events where the sequence number (idn) is greater than startAfter
     * and less than or equal to end, ordered by sequence number.
     *
     * @param startAfter The ID to start fetching events after (exclusive)
     * @param end        The maximum ID to fetch events up to (inclusive)
     * @param maxResults The maximum number of events to fetch
     * @param topic      The topic to fetch events from
     * @return A list of events within the specified range, ordered by sequence
     *         number
     * @throws IllegalArgumentException if startAfter is greater than end,
     *                                  maxResults is less than or equal to 0,
     *                                  or if topic is null or empty
     * @throws RuntimeException         if there is an error accessing the data
     *                                  store
     */
    List<Event> fetchEvents(long startAfter, long end, int maxResults, String topic);

    /**
     * Retrieves the latest message ID (sequence number) for a given topic.
     * This can be used to determine the current position in the event stream
     * and calculate gaps in sequence numbers.
     *
     * @param topic The topic to get the latest message ID for
     * @return The latest message ID for the specified topic, or 0 if no messages
     *         exist
     * @throws IllegalArgumentException if topic is null or empty
     * @throws RuntimeException         if there is an error accessing the data
     *                                  store
     */
    long getLatestMessageId(String topic);
}
