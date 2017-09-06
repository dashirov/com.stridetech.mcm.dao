package com.stridetech.mcm.config;


import com.stridetech.mcm.dao.MCMServiceDao;
import com.stridetech.mcm.dao.MCMServiceDaoPostgreSQL;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import javax.sql.DataSource;
import java.util.Properties;

@Configuration
@EnableAutoConfiguration
@EnableConfigurationProperties
@EnableScheduling
@ConfigurationProperties
@ComponentScan(basePackages = {"com.stridetech.mcm"})
public class ApplicationConfiguration {

    @Bean
    public DataSource createDataSource() {
        final Properties properties=new Properties();

        properties.setProperty("url","jdbc:postgresql://localhost:5432/mcm");
        properties.setProperty("username","davidashirov");
        properties.setProperty("password", "");

        try {
            return BasicDataSourceFactory.createDataSource(new Properties(properties));
        } catch (Exception e){
           e.printStackTrace();
        }
        return null;
    }

    @Bean
    public MCMServiceDao mcmServiceDao(){
        MCMServiceDaoPostgreSQL dao = new MCMServiceDaoPostgreSQL();
        dao.setDatasource(createDataSource());
        return dao;
    }

}
