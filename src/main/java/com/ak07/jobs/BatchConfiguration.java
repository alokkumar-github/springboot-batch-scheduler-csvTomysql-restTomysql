package com.ak07.jobs;

import java.util.Date;

import javax.sql.DataSource;

import org.omg.PortableInterceptor.HOLDING;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ClassPathResource;
import org.springframework.retry.annotation.Retryable;
import org.springframework.scheduling.annotation.Scheduled;

import com.ak07.listener.JobCompletionNotificationListener;
import com.ak07.model.Person;
import com.ak07.processor.PersonItemProcessor;

@Configuration
@EnableBatchProcessing
@Import({ BatchScheduler.class })
public class BatchConfiguration {
	//https://spring.io/guides/gs/batch-processing/
	https://github.com/RawSanj/SpringRestBatch
	@Autowired
	private SimpleJobLauncher jobLauncher;
	
	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	@Autowired
	private DataSource dataSource;

	/**The pattern is a list of six single space-separated fields: 
	 * representing second, minute, hour, day, month, weekday. 
	 * Month and weekday names can be given as the first three letters of the English names.
	 * Example patterns:

	    "0 0 * * * *" = the top of every hour of every day.*
	    "*\/10 * * * * *" = every ten seconds. Remove 2nd character, it is escape
	    "0 0 8-10 * * *" = 8, 9 and 10 o'clock of every day.
	    "0 0/30 8-10 * * *" = 8:00, 8:30, 9:00, 9:30 and 10 o'clock every day.
	    "0 0 9-17 * * MON-FRI" = on the hour nine-to-five weekdays
	    "0 0 0 25 12 ?" = every Christmas Day at midnight

	 */
	//@Scheduled(cron = "1 53/3 17 * * ?")
	@Scheduled(fixedRate = 10000, initialDelay = 5000)
	public void perform() throws Exception {

		System.out.println("Job Started at :" + new Date());

		JobParameters param = new JobParametersBuilder().addString("JobID", String.valueOf(System.currentTimeMillis()))
				.toJobParameters();

		JobExecution execution = jobLauncher.run(importUserJob(null), param);

		System.out.println("Job finished with status :" + execution.getStatus());
	}

	/*@Bean
	public Job processOrderJob() {
		return jobBuilderFactory.get("processOrderJob").incrementer(new RunIdIncrementer()).listener(listener())
				.flow(orderStep()).end().build();
	}*/

	// tag::readerwriterprocessor[]
	
	re
	
	
	@Bean
	public FlatFileItemReader<Person> reader() {
		FlatFileItemReader<Person> reader = new FlatFileItemReader<Person>() /*{
			@Override
	        @Retryable(value = {ItemStreamException.class}, maxAttempts=5)
	        public void open(ExecutionContext executionContext) throws ItemStreamException {
	            super.open(executionContext);
	        }

	        @Override
	        @Retryable(maxAttempts=5)
	        public Person read() throws UnexpectedInputException, ParseException, Exception {
	            return super.read();
	        }
		}*/;
		reader.setResource(new ClassPathResource("person.csv"));
		reader.setLineMapper(new DefaultLineMapper<Person>() {
			{
				setLineTokenizer(new DelimitedLineTokenizer() {
					{
						setNames(new String[] { "firstName", "lastName" });
					}
				});
				setFieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {
					{
						setTargetType(Person.class);
					}
				});
			}
		});
		return reader;
	}

	@Bean
	public PersonItemProcessor processor() {
		return new PersonItemProcessor();
	}

	@Bean
	public JdbcBatchItemWriter<Person> writer() {
		JdbcBatchItemWriter<Person> writer = new JdbcBatchItemWriter<>();
		writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Person>());
		writer.setSql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)");
		writer.setDataSource(dataSource);
		return writer;
	}
	// end::readerwriterprocessor[]

	 // tag::jobstep[]
	  @Bean
	  public Job importUserJob(JobCompletionNotificationListener listener) {
	    return jobBuilderFactory.get("importUserJob").incrementer(new RunIdIncrementer())
	        .listener(listener).flow(step1()).end().build();
	  }

	  @Bean
	  public Step step1() {
	    return stepBuilderFactory.get("step1").<Person, Person>chunk(1).reader(reader())
	        .processor(processor()).writer(writer()).build();
	  }
	// end::jobstep[]
	
}
