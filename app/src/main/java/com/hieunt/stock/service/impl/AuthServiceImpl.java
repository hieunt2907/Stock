package com.hieunt.stock.service.impl;

import java.security.SecureRandom;
import java.time.Duration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.mail.MailException;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;

import com.hieunt.stock.constant.Status;
import com.hieunt.stock.exception.HIEUNTException;
import com.hieunt.stock.model.request.LoginRequest;
import com.hieunt.stock.model.request.RegisterRequest;
import com.hieunt.stock.model.request.VerifyOtpRequest;
import com.hieunt.stock.model.response.AuthFlowResponse;
import com.hieunt.stock.model.response.AuthResponse;
import com.hieunt.stock.repository.RoleRepository;
import com.hieunt.stock.repository.UserRepository;
import com.hieunt.stock.repository.entity.RoleEntity;
import com.hieunt.stock.repository.entity.UserEntity;
import com.hieunt.stock.service.AuthService;
import com.hieunt.stock.util.JwtUtil;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class AuthServiceImpl implements AuthService {

    private static final String LOGIN_OTP_PREFIX = "auth:login:otp:";
    private static final String REGISTER_OTP_PREFIX = "auth:register:otp:";
    private static final String REGISTER_PASSWORD_PREFIX = "auth:register:password:";
    private static final String LOGIN_VERIFY_STEP = "VERIFY_LOGIN_OTP";
    private static final String REGISTER_VERIFY_STEP = "VERIFY_REGISTER_OTP";
    private static final SecureRandom RANDOM = new SecureRandom();

    private final UserRepository userRepository;
    private final RoleRepository roleRepository;
    private final PasswordEncoder passwordEncoder;
    private final JwtUtil jwtUtil;
    private final StringRedisTemplate redisTemplate;
    private final JavaMailSender mailSender;
    private final TemplateEngine templateEngine;

    @Value("${app.auth.otp-expiration-minutes:5}")
    private long otpExpirationMinutes;

    @Value("${app.auth.mail-from:noreply@stock.hieunt.com}")
    private String mailFrom;

    @Override
    public AuthFlowResponse register(RegisterRequest request) throws HIEUNTException {
        String email = normalizeEmail(request.getEmail());
        if (userRepository.existsByEmail(email)) {
            throw new HIEUNTException(HttpStatus.CONFLICT, "Email already exists", "AUTH_001");
        }

        String otp = generateOtp();
        Duration ttl = otpTtl();
        redisTemplate.opsForValue().set(registerOtpKey(email), otp, ttl);
        redisTemplate.opsForValue().set(registerPasswordKey(email), passwordEncoder.encode(request.getPassword()), ttl);
        sendOtpEmail(email, otp, "Stock account registration OTP", "register");

        return AuthFlowResponse.builder()
                .email(email)
                .registered(false)
                .otpSent(true)
                .nextStep(REGISTER_VERIFY_STEP)
                .expiresIn(ttl.toSeconds())
                .build();
    }

    @Override
    public AuthFlowResponse login(LoginRequest request) throws HIEUNTException {
        String email = normalizeEmail(request.getEmail());
        if (!userRepository.existsByEmail(email)) {
            return AuthFlowResponse.builder()
                    .email(email)
                    .registered(false)
                    .otpSent(false)
                    .nextStep("REGISTER_REQUIRED")
                    .expiresIn(0L)
                    .build();
        }

        String otp = generateOtp();
        Duration ttl = otpTtl();
        redisTemplate.opsForValue().set(loginOtpKey(email), otp, ttl);
        sendOtpEmail(email, otp, "Stock login OTP", "login");

        return AuthFlowResponse.builder()
                .email(email)
                .registered(true)
                .otpSent(true)
                .nextStep(LOGIN_VERIFY_STEP)
                .expiresIn(ttl.toSeconds())
                .build();
    }

    @Override
    @Transactional
    public AuthResponse verifyRegisterOtp(VerifyOtpRequest request) throws HIEUNTException {
        String email = normalizeEmail(request.getEmail());
        if (userRepository.existsByEmail(email)) {
            throw new HIEUNTException(HttpStatus.CONFLICT, "Email already exists", "AUTH_001");
        }
        verifyOtp(registerOtpKey(email), request.getOtp());

        String encodedPassword = redisTemplate.opsForValue().get(registerPasswordKey(email));
        if (encodedPassword == null) {
            throw new HIEUNTException(HttpStatus.BAD_REQUEST, "Registration session expired", "AUTH_004");
        }

        UserEntity user = new UserEntity();
        user.setEmail(email);
        user.setPassword(encodedPassword);
        user.setStatus(Status.ACTIVE);
        user.getRoles().add(getOrCreateDefaultUserRole());
        user = userRepository.save(user);

        redisTemplate.delete(registerOtpKey(email));
        redisTemplate.delete(registerPasswordKey(email));

        return jwtUtil.generateAuthResponse(user);
    }

    @Override
    @Transactional(readOnly = true)
    public AuthResponse verifyLoginOtp(VerifyOtpRequest request) throws HIEUNTException {
        String email = normalizeEmail(request.getEmail());
        verifyOtp(loginOtpKey(email), request.getOtp());

        UserEntity user = userRepository.findByEmail(email)
                .orElseThrow(() -> new HIEUNTException(HttpStatus.NOT_FOUND, "User not found", "AUTH_005"));
        redisTemplate.delete(loginOtpKey(email));

        return jwtUtil.generateAuthResponse(user);
    }

    private void verifyOtp(String key, String otp) throws HIEUNTException {
        String expectedOtp = redisTemplate.opsForValue().get(key);
        if (expectedOtp == null) {
            throw new HIEUNTException(HttpStatus.BAD_REQUEST, "OTP expired", "AUTH_003");
        }
        if (!expectedOtp.equals(otp)) {
            throw new HIEUNTException(HttpStatus.UNAUTHORIZED, "Invalid OTP", "AUTH_006");
        }
    }

    private void sendOtpEmail(String email, String otp, String subject, String action) {
        try {
            Context context = new Context();
            context.setVariable("email", email);
            context.setVariable("otp", otp);
            context.setVariable("action", action);
            context.setVariable("expiresInMinutes", otpExpirationMinutes);

            String html = templateEngine.process("mail/otp-email", context);

            MimeMessage message = mailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(message, "UTF-8");
            helper.setFrom(mailFrom);
            helper.setTo(email);
            helper.setSubject(subject);
            helper.setText(html, true);
            mailSender.send(message);
        } catch (MailException | MessagingException ex) {
            log.warn("Could not send OTP email to {}. OTP for development: {}", email, otp, ex);
        }
    }

    private String generateOtp() {
        return String.format("%06d", RANDOM.nextInt(1_000_000));
    }

    private Duration otpTtl() {
        return Duration.ofMinutes(otpExpirationMinutes);
    }

    private String loginOtpKey(String email) {
        return LOGIN_OTP_PREFIX + email;
    }

    private String registerOtpKey(String email) {
        return REGISTER_OTP_PREFIX + email;
    }

    private String registerPasswordKey(String email) {
        return REGISTER_PASSWORD_PREFIX + email;
    }

    private String normalizeEmail(String email) {
        return email.trim().toLowerCase();
    }

    private RoleEntity getOrCreateDefaultUserRole() {
        return roleRepository.findByName("USER")
                .orElseGet(() -> roleRepository.save(new RoleEntity("USER")));
    }
}
