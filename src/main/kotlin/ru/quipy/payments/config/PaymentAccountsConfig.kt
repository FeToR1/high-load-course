package ru.quipy.payments.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.ratelimiter.RateLimiter
import io.github.resilience4j.ratelimiter.RateLimiterConfig
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.sync.Semaphore
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.core.EventSourcingService
import ru.quipy.monitoring.MonitoringService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.PaymentAccountProperties
import ru.quipy.payments.logic.PaymentAggregateState
import ru.quipy.payments.logic.PaymentExternalSystemAdapter
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*


@Configuration
@ConfigurationProperties(prefix = "payment")
class PaymentAccountsConfig {
    private val javaClient = HttpClient.newBuilder().build()
    private val mapper = ObjectMapper().registerKotlinModule().registerModules(JavaTimeModule())

    lateinit var paymentProviderHostPort: String
    lateinit var serviceName: String
    lateinit var token: String

    @Value("#{'\${payment.accounts}'.split(',')}")
    lateinit var allowedAccounts: List<String>

    @Bean
    fun dbScope(
        @Qualifier("eventSourcingDispatcher")
        esDispatcher: ExecutorCoroutineDispatcher
    ) = CoroutineScope(SupervisorJob() + Dispatchers.IO + esDispatcher)

    @Bean
    fun accountAdapters(
        paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
        monitoringService: MonitoringService,
        @Qualifier("eventSourcingDispatcher")
        esDispatcher: ExecutorCoroutineDispatcher,
        dbScope: CoroutineScope
    ): List<PaymentExternalSystemAdapter> {
        val request = HttpRequest.newBuilder()
            .uri(URI("http://${paymentProviderHostPort}/external/accounts?serviceName=$serviceName&token=$token"))
            .GET()
            .build()

        val resp = javaClient.send(request, HttpResponse.BodyHandlers.ofString())

        println("\nPayment accounts list:")
        return mapper.readValue<List<PaymentAccountProperties>>(
            resp.body(),
            mapper.typeFactory.constructCollectionType(List::class.java, PaymentAccountProperties::class.java)
        )
            .filter { it.accountName in allowedAccounts }
            .map { it.copy(enabled = true) }
            .onEach(::println)
            .map {
                PaymentExternalSystemAdapter(
                    it,
                    paymentESService,
                    paymentProviderHostPort,
                    token,
                    monitoringService,
                    Semaphore(it.parallelRequests),
                    RateLimiter.of("rate-limiter", RateLimiterConfig.custom()
                        .limitForPeriod(it.rateLimitPerSec)
                        .limitRefreshPeriod(Duration.ofMillis(1000))
                        .build()
                    ),
                    dbScope
                )
            }
    }
}