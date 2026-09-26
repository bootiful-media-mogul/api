-- one set of JobRunr tables backs every service, and each node only runs the jobs whose
-- jobSignature it recognizes (see the jobrunr module's SignatureFilteringStorageProvider).
-- that makes "how much work is waiting for the processors" a per-signature question --
-- and a signature is a java class and method name, which is not something a kubernetes
-- manifest should ever know. rename a handler and the manifest would go on quietly
-- counting nothing, with nothing to complain to.
--
-- so name the thing that doesn't move: the module. every signature reads
-- com.joshlong.mogul.<module>...., and <module> is what the deployment is called.
--
-- the zero is this view's job, not its callers'. a plain GROUP BY answers a module with
-- nothing in flight by saying nothing at all, and an autoscaler reading a single value
-- gets an empty result where it wanted a 0 -- broken at precisely the quietest moment. so
-- take the modules from the whole table and left join the work onto them: every module the
-- table knows about has a row, whether or not it is busy.
create or replace view jobrunr_jobs_by_module as
with modules as (select distinct substring(jobsignature from 'mogul\.([^.]+)') as module
                 from jobrunr_jobs
                 where jobsignature like '%mogul.%'),
     in_flight as (select substring(jobsignature from 'mogul\.([^.]+)') as module,
                          count(*)                                     as depth
                   from jobrunr_jobs
                   -- PROCESSING counts, not just ENQUEUED: the moment the workers claim a
                   -- backlog the enqueued count is zero, and an autoscaler watching only
                   -- that would take the pods away from the jobs it just asked for.
                   where state in ('ENQUEUED', 'PROCESSING')
                     and jobsignature like '%mogul.%'
                   group by 1)
select m.module,
       coalesce(f.depth, 0) as depth
from modules m
         left join in_flight f using (module);

comment on view jobrunr_jobs_by_module is
    'queued-or-running jobs per mogul module, for autoscaling. one row per module the jobs table has seen, 0 when idle.';
