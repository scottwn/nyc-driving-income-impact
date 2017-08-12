select * from analytic_table order by analytic_table.commuters desc;
select * from analytic_table order by analytic_table.crashes desc;
select * from analytic_table order by analytic_table.complaints desc;
select avg(analytic_table.commuters) avg_commuters from analytic_table;
select avg(analytic_table.crashes) avg_crashes from analytic_table;
select avg(analytic_table.complaints) avg_complaints from analytic_table;
select * from analytic_table where(analytic_table.commuters > 25 and analytic_table.crashes > 1160 and analytic_table.complaints > 11870);