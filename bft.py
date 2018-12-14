#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import sqlite3
import logging

'''
$ sqlite3
SQLite version 3.25.3 2018-11-05 20:37:38
Enter ".help" for usage hints.
Connected to a transient in-memory database.
Use ".open FILENAME" to reopen on a persistent database.
sqlite> create table company(id int, name text);
sqlite> create table summary(id int, total int);
sqlite> insert into summary (id, total) values (0, 0);
sqlite> select * from summary;
0|0
sqlite> create trigger before_insert_company before insert on company for each row begin select raise(abort, 'dup') where exists (select 1 from company where id = NEW.id); end;
sqlite> create trigger after_insert_company after insert on company for each row begin update summary set total = total + 1 where id = 0; end;
sqlite> insert into company (id, name) values (0, 'haha');
sqlite> select * from summary;
0|1
sqlite> select * from company;
0|haha
sqlite> insert into company (id, name) values (0, 'heihei');
Error: dup
sqlite> select * from company;
0|haha
sqlite> select * from summary;
0|1
sqlite> insert into company (id, name) values (1, 'heihei');
sqlite> select * from summary;
0|2
sqlite> select * from company;
0|haha
1|heihei
sqlite> 
'''


def main():
    # create db
    logging.info("create db bft.db")
    bftdb = sqlite3.connect('bft.db')
    cursor = bftdb.cursor()
    # create tables
    logging.info("create db tables")
    cursor.execute('create table proposers(height int, count int, proposer_list blob);')
    cursor.execute('create table validators(height int, count int, threshold int, validator blob);')
    cursor.execute('create table blocks(height int, round int, proposal_hash text, block blob);')
    cursor.execute('create table proposals(height int, round int, proposal_hash text, signature text, sender text);')
    cursor.execute('create table pre_votes(height int, round int, proposal_hash text, signature text, sender text);')
    cursor.execute('create table pre_commits(height int, round int, proposal_hash text, signature text, sender text);')
    cursor.execute('create table lock_info(height int primary key, proposal_hash text, lock_round int);')
    cursor.execute('create table commit_info(height int primary key, proposal_hash text);')
    cursor.execute('create table chain_status(ii int, height int, block_hash text);')
    cursor.execute('create table actions(height int, round int, action text);')

    # install trigger
    logging.info("install trigger")
    cursor.execute('create trigger before_insert_proposals before insert \
    on proposals \
    for each row \
    begin \
    select raise(abort, "dup") \
    where exists (select 1 from proposals where height = NEW.height and round = NEW.round); \
    end;')
    cursor.execute('create trigger before_insert_pre_votes before insert \
    on pre_votes \
    for each row \
    begin \
    select raise(abort, "dup") \
    where exists (select 1 from pre_votes where height = NEW.height and round = NEW.round and sender = NEW.sender); \
    end;')
    cursor.execute('create trigger before_insert_pre_commits before insert \
    on pre_commits \
    for each row \
    begin \
    select raise(abort, "dup") \
    where exists (select 1 from pre_commits where height = NEW.height and round = NEW.round and sender = NEW.sender); \
    end;')
    cursor.execute('create trigger after_insert_proposals after insert \
    on proposals \
    for each row \
    begin \
    update proposals set \
    proposal_hash = (select proposal_hash from lock_info where height = NEW.height), \
    signature = "", \
    sender = "me" \
    where height = NEW.height and round = NEW.round; \
    update proposals set proposal_hash =  NEW.proposal_hash \
    where height = NEW.height and round = NEW.round and proposal_hash is null; \
    insert into pre_votes \
    select * from proposals where height = NEW.height and round = NEW.round; \
    end;')
    cursor.execute('create trigger after_insert_pre_votes after insert \
    on pre_votes \
    for each row \
    when (select COUNT(*) from pre_votes \
    where height = NEW.height and round = NEW.round and proposal_hash = NEW.proposal_hash) > \
    (select threshold from validators where height = NEW.height) \
    begin \
    insert into pre_commits (height, round, proposal_hash, signature, sender) \
    values (NEW.height, NEW.round, NEW.proposal_hash, "", "me"); \
    insert into lock_info (height, proposal_hash, lock_round) values (NEW.height, NEW.proposal_hash, NEW.round) \
    on conflict(height) do update set \
    proposal_hash = excluded.proposal_hash, \
    lock_round = excluded.lock_round \
    where excluded.lock_round > lock_info.lock_round; \
    delete from lock_info where proposal_hash = ""; \
    end;')
    cursor.execute('create trigger after_insert_pre_commits after insert \
    on pre_commits \
    for each row \
    when (select COUNT(*) from pre_commits \
    where height = NEW.height and round = NEW.round and proposal_hash = NEW.proposal_hash) > \
    (select threshold from validators where height = NEW.height) \
    begin \
    insert into commit_info (height, proposal_hash) values (NEW.height, NEW.proposal_hash); \
    end;')
    cursor.execute('create trigger after_insert_chain_status after update \
    on chain_status \
    for each row \
    begin \
    delete from proposers where height <= NEW.height - 2; \
    delete from validators where height <= NEW.height - 2; \
    delete from blocks where height <= NEW.height; \
    delete from proposals where height <= NEW.height; \
    delete from pre_votes where height <= NEW.height; \
    delete from pre_commits where height <= NEW.height - 1; \
    delete from lock_info where height <= NEW.height; \
    delete from commit_info where height <= NEW.height; \
    delete from actions where height <= NEW.height; \
    end;')

    logging.info("db init complete")
    # begin test
    test(cursor)


def test(cursor):
    test_after_proposals_1(cursor)


def test_init(cursor):
    # get chain_status
    cursor.execute('insert into chain_status (ii, height, block_hash) values (0, 0, "0x0000")')
    cursor.execute('insert into proposers (height, count, proposer_list) values (0, 4, "0123")')
    cursor.execute('insert into validators (height, count, threshold, validator) values (0, 4, 2, "1234")')


def test_after_proposals_0(cursor):
    # get chain_status
    test_init(cursor)
    # get proposal
    cursor.execute('insert into proposals (height, round, proposal_hash, signature, sender) \
    values (1, 0, "0x1111", "0xaaaa", "0")')
    # check pre_votes
    cursor.execute('select * from pre_votes where height = ?', (1,))
    print(cursor.fetchall())


def test_after_proposals_1(cursor):
    test_init(cursor)
    # set lock info
    cursor.execute('insert into lock_info (height, proposal_hash, lock_round) \
    values (1, "0x2222", 0)')
    # get proposal
    cursor.execute('insert into proposals (height, round, proposal_hash, signature, sender) \
    values (1, 0, "0x1111", "0xaaaa", "0")')
    # check pre_votes
    cursor.execute('select * from pre_votes where height = ?', (1,))
    print(cursor.fetchall())


if __name__ == '__main__':
    main()
