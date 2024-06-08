package dev.mayankg.util;

import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import dev.mayankg.dto.Customer;
import org.springframework.core.io.ClassPathResource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

public class CsvReader {

    public static List<Customer> readDataFromCsv() {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader
                (new ClassPathResource("users.csv").getInputStream()))) {
            CsvToBean<Customer> csvToBean = new CsvToBeanBuilder<Customer>(reader)
                    .withType(Customer.class)
                    .build();
            return csvToBean.parse();
        } catch (IOException e) {
            e.printStackTrace();
            // Handle the exception as needed
            return null;
        }
    }
}