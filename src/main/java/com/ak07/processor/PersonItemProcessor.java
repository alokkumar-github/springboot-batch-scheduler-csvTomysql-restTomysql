package com.ak07.processor;

import com.ak07.model.Person;
import lombok.extern.slf4j.Slf4j;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

@Slf4j
public class PersonItemProcessor implements ItemProcessor<Person, Person> {
	private final Logger log = LoggerFactory.getLogger(PersonItemProcessor.class);
  @Override
  public Person process(Person person) throws Exception {
    final String firstName = person.getFirstName().toUpperCase();
    final String lastName = person.getLastName().toUpperCase();
    final Person transformedPerson = new Person(firstName, lastName);
    //log.info("Converting (" + person + ") into (" + transformedPerson + ")");

    return transformedPerson;
  }
}