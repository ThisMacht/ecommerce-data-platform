-- Same review ID across orders and several reviews for one order are independent keys.
INSERT INTO olist.order_reviews
    (review_id,order_id,review_score,review_comment_title,review_comment_message,
     review_creation_date,review_answer_timestamp,source_updated_at,source_operation,source_batch_id)
VALUES
    ('r','order_a',5,NULL,E' Ótimo!\nCHEGOU rápido. ','2020-01-01','2020-01-02 10:15:00.123456','2020-01-04','INSERT','fixture'),
    ('r','order_b',4,NULL,NULL,'2020-01-01','2020-01-02','2020-01-01','INSERT','fixture'),
    ('second','order_a',3,' ',' ','2020-01-01','2020-01-02','2020-01-02','INSERT','fixture'),
    ('clock','order_a',1,NULL,NULL,'2020-01-02','2020-01-01','2020-01-01','INSERT','fixture'),
    ('late1','missing',2,NULL,'frozen','2020-01-01','2020-01-02','2020-01-01','INSERT','fixture'),
    ('late2','missing',4,NULL,NULL,'2020-01-01','2020-01-02','2020-01-02','INSERT','fixture'),
    ('expire','never',2,NULL,NULL,'2020-01-01','2020-01-02','2020-01-01','INSERT','fixture'),
    ('bad','missing',0,NULL,NULL,'2020-01-01','2020-01-02','2020-01-02','INSERT','fixture');
