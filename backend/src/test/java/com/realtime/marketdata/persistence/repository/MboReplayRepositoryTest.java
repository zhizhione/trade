package com.realtime.marketdata.persistence.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.zaxxer.hikari.HikariDataSource;
import com.realtime.marketdata.replay.source.ReplayDataAccessException;
import java.net.SocketException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

class MboReplayRepositoryTest {

    @Test
    void retriesTheCatalogOnceAfterAConnectionReset() throws Exception {
        MboReplayRepository repository = new MboReplayRepository(
            true,
            "jdbc:ch:http://localhost:8123/market_data",
            "market",
            "unused"
        );
        HikariDataSource dataSource = mock(HikariDataSource.class);
        Connection resetConnection = mock(Connection.class);
        PreparedStatement resetStatement = mock(PreparedStatement.class);
        when(resetConnection.prepareStatement(anyString())).thenReturn(resetStatement);
        when(resetStatement.executeQuery()).thenThrow(
            new SQLException("Connection reset", new SocketException("Connection reset"))
        );

        Connection recoveredConnection = mock(Connection.class);
        PreparedStatement catalogStatement = mock(PreparedStatement.class);
        PreparedStatement identityStatement = mock(PreparedStatement.class);
        ResultSet emptyCatalog = mock(ResultSet.class);
        when(recoveredConnection.prepareStatement(anyString())).thenReturn(catalogStatement, identityStatement);
        when(catalogStatement.executeQuery()).thenReturn(emptyCatalog);
        when(emptyCatalog.next()).thenReturn(false);
        when(dataSource.getConnection()).thenReturn(resetConnection, recoveredConnection);
        ReflectionTestUtils.setField(repository, "dataSource", dataSource);

        assertThat(repository.catalog()).isEmpty();

        verify(dataSource, times(2)).getConnection();
    }

    @Test
    void streamsOnlyTheRequestedIdentityWithCatalogOrdinalBounds() throws Exception {
        MboReplayRepository repository = new MboReplayRepository(
            true,
            "jdbc:ch:http://localhost:8123/market_data",
            "market",
            "unused"
        );
        HikariDataSource dataSource = mock(HikariDataSource.class);
        Connection connection = mock(Connection.class);
        PreparedStatement rangesStatement = mock(PreparedStatement.class);
        PreparedStatement resetStatement = mock(PreparedStatement.class);
        PreparedStatement rawStatement = mock(PreparedStatement.class);
        ResultSet rangeRows = mock(ResultSet.class);
        ResultSet resetRows = mock(ResultSet.class);
        ResultSet rawRows = mock(ResultSet.class);
        String fileSha256 = "a".repeat(64);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(rangesStatement, resetStatement, rawStatement);
        when(rangesStatement.executeQuery()).thenReturn(rangeRows);
        when(rangeRows.next()).thenReturn(true, false);
        when(rangeRows.getString(1)).thenReturn(fileSha256);
        when(rangeRows.getLong(2)).thenReturn(100L);
        when(rangeRows.getLong(3)).thenReturn(200L);
        when(rangeRows.getLong(4)).thenReturn(9L);
        when(rangeRows.wasNull()).thenReturn(false);
        when(resetStatement.executeQuery()).thenReturn(resetRows);
        when(resetRows.next()).thenReturn(true);
        when(resetRows.getLong(1)).thenReturn(0L);
        when(resetRows.wasNull()).thenReturn(false);
        when(rawStatement.executeQuery()).thenReturn(rawRows);
        when(rawRows.next()).thenReturn(false);
        ReflectionTestUtils.setField(repository, "dataSource", dataSource);

        assertThat(repository.streamEvents(7, 42L, 0L, 1L, event -> true)).isTrue();

        verify(rangesStatement).setInt(1, 7);
        verify(rangesStatement).setLong(2, 42L);
        verify(rawStatement).setString(1, fileSha256);
        verify(rawStatement).setInt(2, 7);
        verify(rawStatement).setLong(3, 42L);
        verify(rawStatement).setLong(4, 1_000_000L);

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(connection, times(3)).prepareStatement(sql.capture());
        assertThat(sql.getAllValues().get(0))
            .contains("publisher_id = ?")
            .contains("instrument_id = ?")
            .contains("max(catalog.last_source_ordinal)")
            .contains("coalesce(catalog.min_ts_event, catalog.first_ts_event)")
            .contains("coalesce(catalog.max_ts_event, catalog.last_ts_event)")
            .contains("NULLS LAST");
        assertThat(sql.getAllValues().get(1))
            .contains("action = 'R'")
            .contains("PREWHERE file_sha256 = toFixedString(?, 64)");
        assertThat(sql.getAllValues().get(2))
            .contains("PREWHERE file_sha256 = toFixedString(?, 64)");
    }

    @Test
    void rejectsAReplayWhenNoResetExistsInTheWarmupChain() throws Exception {
        MboReplayRepository repository = new MboReplayRepository(
            true,
            "jdbc:ch:http://localhost:8123/market_data",
            "market",
            "unused"
        );
        HikariDataSource dataSource = mock(HikariDataSource.class);
        Connection connection = mock(Connection.class);
        PreparedStatement rangesStatement = mock(PreparedStatement.class);
        PreparedStatement resetStatement = mock(PreparedStatement.class);
        ResultSet rangeRows = mock(ResultSet.class);
        ResultSet resetRows = mock(ResultSet.class);
        String fileSha256 = "b".repeat(64);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(rangesStatement, resetStatement);
        when(rangesStatement.executeQuery()).thenReturn(rangeRows);
        when(rangeRows.next()).thenReturn(true, false);
        when(rangeRows.getString(1)).thenReturn(fileSha256);
        when(rangeRows.getLong(2)).thenReturn(100_000_000L);
        when(rangeRows.getLong(3)).thenReturn(200_000_000L);
        when(rangeRows.getLong(4)).thenReturn(9L);
        when(rangeRows.wasNull()).thenReturn(false);
        when(resetStatement.executeQuery()).thenReturn(resetRows);
        when(resetRows.next()).thenReturn(true);
        when(resetRows.getLong(1)).thenReturn(0L);
        when(resetRows.wasNull()).thenReturn(true);
        ReflectionTestUtils.setField(repository, "dataSource", dataSource);

        assertThatThrownBy(() -> repository.streamEvents(7, 42L, 150L, 200L, event -> true))
            .isInstanceOf(ReplayDataAccessException.class)
            .hasMessageContaining("no reset event found");
    }

    @Test
    void walksBackAcrossFilesUntilItFindsTheNearestReset() throws Exception {
        MboReplayRepository repository = new MboReplayRepository(
            true,
            "jdbc:ch:http://localhost:8123/market_data",
            "market",
            "unused"
        );
        HikariDataSource dataSource = mock(HikariDataSource.class);
        Connection connection = mock(Connection.class);
        PreparedStatement rangesStatement = mock(PreparedStatement.class);
        PreparedStatement resetStatement = mock(PreparedStatement.class);
        PreparedStatement rawStatement = mock(PreparedStatement.class);
        ResultSet rangeRows = mock(ResultSet.class);
        ResultSet resetAbsent = mock(ResultSet.class);
        ResultSet resetPresent = mock(ResultSet.class);
        ResultSet emptyRows = mock(ResultSet.class);
        String firstFile = "c".repeat(64);
        String resetFile = "d".repeat(64);
        String overlapFile = "e".repeat(64);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(
            rangesStatement, resetStatement, rawStatement, rawStatement
        );
        when(rangesStatement.executeQuery()).thenReturn(rangeRows);
        when(rangeRows.next()).thenReturn(true, true, true, false);
        when(rangeRows.getString(1)).thenReturn(firstFile, resetFile, overlapFile);
        when(rangeRows.getLong(2)).thenReturn(0L, 100_000_000L, 200_000_000L);
        when(rangeRows.getLong(3)).thenReturn(100_000_000L, 200_000_000L, 300_000_000L);
        when(rangeRows.getLong(4)).thenReturn(9L, 19L, 29L);
        when(rangeRows.wasNull()).thenReturn(false);
        when(resetStatement.executeQuery()).thenReturn(resetAbsent, resetPresent);
        when(resetAbsent.next()).thenReturn(true);
        when(resetAbsent.getLong(1)).thenReturn(0L);
        when(resetAbsent.wasNull()).thenReturn(true);
        when(resetPresent.next()).thenReturn(true);
        when(resetPresent.getLong(1)).thenReturn(2L);
        when(resetPresent.wasNull()).thenReturn(false);
        when(rawStatement.executeQuery()).thenReturn(emptyRows);
        when(emptyRows.next()).thenReturn(false);
        ReflectionTestUtils.setField(repository, "dataSource", dataSource);

        assertThat(repository.streamEvents(7, 42L, 250L, 300L, event -> true)).isTrue();

        ArgumentCaptor<String> files = ArgumentCaptor.forClass(String.class);
        verify(rawStatement, times(2)).setString(org.mockito.ArgumentMatchers.eq(1), files.capture());
        assertThat(files.getAllValues()).containsExactly(resetFile, overlapFile);
    }
}
