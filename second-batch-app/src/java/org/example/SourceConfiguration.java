package org.example;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;


@Configuration // Configuration for source DB
@PropertySource("classpath:source.properties")
public class SourceConfiguration {

    @Bean
    @Qualifier("sourceDataSource")
    public DataSource sourceDataSource(@Value("${db.src.url}") String url,
                                       @Value("${db.src.username}") String username,
                                       @Value("${db.src.password}") String password) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();

        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        return dataSource;
    }
}
