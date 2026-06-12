package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import com.jpmc.midascore.service.IncentiveService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class TransactionListener {

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;
    private final IncentiveService incentiveService;

    public TransactionListener(
            UserRepository userRepository,
            TransactionRecordRepository transactionRecordRepository,
            IncentiveService incentiveService
    ) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
        this.incentiveService = incentiveService;
    }

    @Transactional
    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core")
    public void listen(Transaction transaction) {
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        if (sender == null || recipient == null) {
            return;
        }

        if (sender.getBalance() < transaction.getAmount()) {
            return;
        }

        Incentive incentive = incentiveService.getIncentive(transaction);
        float incentiveAmount = incentive == null ? 0 : incentive.getAmount();

        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

        userRepository.save(sender);
        userRepository.save(recipient);

        TransactionRecord record = new TransactionRecord(
                sender,
                recipient,
                transaction.getAmount(),
                incentiveAmount
        );

        transactionRecordRepository.save(record);

        if ("wilbur".equalsIgnoreCase(sender.getName()) || "wilbur".equalsIgnoreCase(recipient.getName())) {
            UserRecord wilbur = userRepository.findByName("wilbur");
            if (wilbur != null) {
                System.out.println("======================================");
                System.out.println("WILBUR_BALANCE = " + wilbur.getBalance());
                System.out.println("WILBUR_ROUNDED_DOWN = " + (int) Math.floor(wilbur.getBalance()));
                System.out.println("======================================");
            }
        }
    }
}