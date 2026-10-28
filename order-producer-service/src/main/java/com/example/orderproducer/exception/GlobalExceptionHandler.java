package com.example.orderproducer.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Converts exceptions thrown by any controller into consistent JSON error responses.
 *
 * <p>{@code @RestControllerAdvice} applies these {@code @ExceptionHandler} methods to every
 * {@code @RestController}. The responses use {@link ProblemDetail} (RFC 9457 "problem+json"),
 * the standard error format supported by Spring 6.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    /** 400 - one or more @Valid constraints failed. Lists every invalid field. */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail handleValidation(MethodArgumentNotValidException ex) {
        Map<String, String> errors = new LinkedHashMap<>();
        ex.getBindingResult().getFieldErrors()
                .forEach(error -> errors.putIfAbsent(error.getField(), error.getDefaultMessage()));

        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.BAD_REQUEST, "Request validation failed");
        problem.setTitle("Invalid order request");
        problem.setProperty("errors", errors);
        return problem;
    }

    /** 400 - the body is not valid JSON, or a field has the wrong type (e.g. "price": "abc"). */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ProblemDetail handleUnreadable(HttpMessageNotReadableException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.BAD_REQUEST, "Request body is missing or is not valid JSON");
        problem.setTitle("Malformed request");
        return problem;
    }

    /** 503 - the request was fine but Kafka could not confirm the event. The client may retry. */
    @ExceptionHandler(OrderPublishingException.class)
    public ProblemDetail handlePublishing(OrderPublishingException ex) {
        log.error("Order publishing failed", ex);
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.SERVICE_UNAVAILABLE, "Order could not be published. Please retry later.");
        problem.setTitle("Messaging system unavailable");
        return problem;
    }
}
