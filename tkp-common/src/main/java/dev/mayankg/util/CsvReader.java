package dev.mayankg.util;

import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import dev.mayankg.dto.Customer;
import org.springframework.core.io.ClassPathResource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CsvReader {

    private static final Logger LOGGER = Logger.getLogger(CsvReader.class.getName());

    public static List<Customer> readDataFromCsv() {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                new ClassPathResource("users.csv").getInputStream()))) {
            CsvToBean<Customer> csvToBean = new CsvToBeanBuilder<Customer>(reader)
                    .withType(Customer.class)
                    .build();
            return csvToBean.parse();
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Error reading CSV file", e);
            // Return an empty list or throw a custom exception if appropriate
            return Collections.emptyList();
        }
    }
}