import { defineService } from '../../../kernel/define-service';

export default defineService('Comments', () => {
  const CommentsService = require('./comments-service');
  const CommentsController = require('./comments-controller');
  const CommentsStats = require('./comments-stats-service');

  const config = require('../../../shared/config');
  const logging = require('@tryghost/logging');
  const models = require('../../models');
  const { GhostMailer } = require('../../lib/mail');
  const settingsCache = require('../../../shared/settings-cache');
  const settingsHelpers = require('../settings-helpers');
  const urlService = require('../url');
  const urlUtils = require('../../../shared/url-utils').default;
  const membersService = require('../members');
  const db = require('../../data/db');
  const DomainEvents = require('@tryghost/domain-events');
  const { MemberCommentEvent } = require('../../../shared/events');
  const { byNQL } = require('../../models/base/plugins/bulk-filters');
  const { t } = require('../i18n');
  const emailService = require('../email-service');

  const api = new CommentsService({
    config,
    logging,
    models,
    mailer: new GhostMailer(),
    settingsCache,
    settingsHelpers,
    urlService,
    urlUtils,
    contentGating: membersService.contentGating,
    DomainEvents,
    MemberCommentEvent,
    byNQL,
    t,
    // Email's renderer exists only once that service has initialized, which
    // happens alongside this one; resolve it when a reply notification is sent.
    createUnsubscribeUrl: (uuid: string, options: { comments: boolean }) =>
      emailService.renderer.createUnsubscribeUrl(uuid, options),
  });
  const controller = new CommentsController(api, new CommentsStats({ db }));

  return { api, controller };
});
