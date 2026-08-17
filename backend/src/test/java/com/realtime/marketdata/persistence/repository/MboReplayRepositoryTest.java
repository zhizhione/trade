package com.realtime.marketdata.persistence.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.zaxxer.hikari.HikariDataSource;
import java.net.SocketException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;
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
}
