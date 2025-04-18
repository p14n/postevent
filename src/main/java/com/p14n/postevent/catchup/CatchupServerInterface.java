package com.p14n.postevent.catchup;

import com.p14n.postevent.data.Event;
import java.util.List;

/**
 * Interface for fetching events from a data store.
 */
public interface CatchupServerInterface {

    /**
     * Fetches events from a data store within a specified range.
     *
     * @param startAfter The ID to start fetching events after (exclusive)
     * @param end        The maximum ID to fetch events up to (inclusive)
     * @param maxResults The maximum number of events to fetch
     * @return A list of events within the specified range
     */
    List<Event> fetchEvents(long startAfter, long end, int maxResults, String topic);
}