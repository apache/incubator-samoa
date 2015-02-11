---
title: Apache SAMOA Bylaws
layout: documentation
documentation: true
---
# SAMOA Bylaws (Draft)

This document defines the bylaws under which the Apache SAMOA project operates.

Apache SAMOA is a project of the Apache Software Foundation (ASF). The foundation holds the copyright on the code in the Apache SAMOA codebase. The foundation [FAQ](http://www.apache.org/foundation/faq.html) explains the operation and background of the foundation.

Apache SAMOA is typical of Apache projects in that it operates under a set of principles, known collectively as the *Apache Way*. If you are new to Apache development, please refer to the [Apache Incubator](http://incubator.apache.org) for more information on how Apache projects operate.


## Roles and Responsibilities

Apache projects define a set of roles with associated rights and responsibilities. These roles govern which tasks an individual may perform within the project. The roles are defined in the following sections.

### Users:

The most important participants in the project are people who use our software. The majority of our developers start out as users and guide their development efforts from the user's perspective.

Users contribute to Apache projects by providing feedback to developers in the form of bug reports and feature suggestions. In addition, users participate in the Apache community by helping other users on mailing lists and user support forums.

### Contributors:

All of the volunteers who are contributing time, code, documentation, or resources to the SAMOA project. A contributor that makes sustained, welcome contributions to the project may be invited to become a Committer, though the exact timing of such invitations depends on many factors.

### Committers:

The project's Committers are responsible for the project's technical management. Committers have access to all project source repositories. Committers may cast binding votes on any technical discussion regarding SAMOA.

Committer access is by invitation only and must be approved by lazy consensus of the active PMC members. A Committer is considered emeritus by their own declaration or by not contributing in any form to the project for over six months. An emeritus Committer may request reinstatement of commit access from the PMC. Such reinstatement is subject to lazy consensus approval of active PMC members.

All Apache Committers are required to have a signed [Contributor License Agreement (CLA)](http://www.apache.org/licenses/icla.txt) on file with the Apache Software Foundation. There is a [Committer FAQ](http://www.apache.org/dev/committers.html) which provides more details on the requirements for Committers

A Committer who makes a sustained contribution to the project may be invited to become a member of the PMC. The form of contribution is not limited to code. It can also include other activities such as code review, helping out users on the mailing lists, documentation, and testing.

### Project Management Committee (PMC):

The PMC is responsible to the board and the ASF for the management and oversight of the Apache SAMOA codebase. The responsibilities of the PMC include

 * Deciding what is distributed as products of the Apache SAMOA project. In particular all releases must be approved by the PMC.
 * Maintaining the project's shared resources, including the codebase repository, mailing lists, websites.
 * Speaking on behalf of the project.
 * Resolving license disputes regarding products of the project.
 * Nominating new PMC members and Committers.
 * Maintaining these bylaws and other guidelines of the project.

Membership of the PMC is by invitation only and must be approved by consensus approval of the active PMC members. A PMC member is considered "emeritus" by their own declaration or by not contributing in any form to the project for over six months. An emeritus member may request reinstatement to the PMC. Such reinstatement is subject to consensus approval of the active PMC members.

The chair of the PMC is appointed by the ASF board. The chair is an office holder of the Apache Software Foundation (Vice President, Apache SAMOA) and has primary responsibility to the board for the management of the projects within the scope of the SAMOA PMC. The chair reports to the board quarterly on developments within the SAMOA project.

The chair of the PMC is rotated annually. When the chair is rotated, or if the current chair of the PMC resigns, the PMC votes to recommend a new chair using [Single Transferable Vote (STV)](http://wiki.apache.org/general/BoardVoting) voting. The decision must be ratified by the ASF board.

## Voting

Decisions regarding the project are made by votes on the primary project development mailing list (dev@samoa.incubator.apache.org). Where necessary, PMC voting may take place on the private SAMOA PMC mailing list. Votes are clearly indicated by subject line starting with [VOTE]. Votes may contain multiple items for approval and these should be clearly separated. Voting is carried out by replying to the vote mail. Voting may take four flavors.

| Vote | Meaning |
|------|---------|
| +1   | 'Yes', 'Agree', or 'the action should be performed'. |
| +0   | Neutral about the proposed action (or mildly negative but not enough so to want to block it). |
| -1   | This is a negative vote. On issues where consensus is required, this vote counts as a veto. All vetoes must contain an explanation of why the veto is appropriate. Vetoes with no explanation are void. It may also be appropriate for a -1 vote to include an alternative course of action. |

All participants in the SAMOA project are encouraged to show their agreement with or against a particular action by voting. For technical decisions, only the votes of active Committers are binding. Non-binding votes are still useful for those with binding votes to understand the perception of an action in the wider SAMOA community. For PMC decisions, only the votes of active PMC members are binding.

Voting can also be applied to changes already made to the SAMOA codebase. These typically take the form of a veto (-1) in reply to the commit message sent when the commit is made. Note that this should be a rare occurrence. All efforts should be made to discuss issues when they are still patches before the code is committed.

Only active (i.e., non-emeritus) Committers and PMC members have binding votes.

## Approvals

These are the types of approval that can be sought. Different actions require different types of approval.

| Approval          | Requirements |
|-------------------|--------------|
| Consensus         | requires all binding-vote holders to cast +1 votes and no binding -1 vetoes (consensus votes are rarely required due to the impracticality of getting all eligible voters to cast a vote). |
| 2/3 Majority      | requires at least 2/3 of binding-vote holders to cast +1 votes. (2/3 majority is typically used for actions that affect the foundation of the project, e.g., adopting a new codebase to replace an existing product). |
| Lazy Consensus    | requires 2 binding +1 votes and no -1 votes ('silence gives assent'). |
| Lazy Majority     | requires 3 binding +1 votes and more binding +1 votes than -1 vetoes. |
| Lazy 2/3 Majority | requires at least 3 votes and twice as many +1 votes as -1 vetoes. |

### Vetoes

A valid, binding veto cannot be overruled. If a veto is cast, it must be accompanied by a valid reason explaining the reasons for the veto. The validity of a veto, if challenged, can be confirmed by anyone who has a binding vote. This does not necessarily signify agreement with the veto - merely that the veto is valid.

If you disagree with a valid veto, you must lobby the person casting the veto to withdraw their veto. If a veto is not withdrawn, any action that has been vetoed must be reversed in a timely manner.

## Actions

This section describes the various actions which are undertaken within the project, the corresponding approval required for that action and those who have binding votes over the action.

| Action | Description | Approval | Binding Votes | Minimum Length | Mailing List |
|--------|-------------|----------|---------------|----------------|--------------|
| Code Change | A change made to a codebase of the project and committed by a committer. This includes source code, documentation, and website content. | Lazy Consensus (with at least one +1 vote from someone who has not authored the patch). The code can be committed as soon as the required number of binding votes is reached. | Active Committers | 1 day | JIRA or GitHub pull request (with notification sent to dev@) |
| Release Plan | Defines the timetable and actions for a release. The plan also nominates a Release Manager. | Lazy Majority | Active Committers | 3 days | dev@ |
| Product Release | Accepting the official release of a product of the project. | Lazy Majority | Active PMC members | 3 days | dev@ |
| Adoption of New Codebase | Replacing the codebase for an existing, released product with an alternative codebase. If such a vote fails to gain approval, the existing code base will continue. This action also covers the creation of new sub-projects and sub-modules within the project. | Lazy 2/3 Majority | Active PMC members | 7 days | dev@ |
| New Committer | Electing a new Committer for the project. | Lazy Consensus | Active PMC members | 7 days | private@ |
| New PMC Member | Promoting a Committer to the PMC of the project. | Consensus | Active PMC members | 7 days | private@ |
| Emeritus PMC Member re-instatement | When an emeritus PMC member requests to be re-instated as an active PMC member. | Consensus | Active PMC members | 7 days | private@ |
| Emeritus Committer re-instatement | When an emeritus Committer requests to be re-instated as an active committer. | Consensus | Active PMC members | 7 days | private@ |
| Committer Removal | When removal of commit privileges is sought. Note: Such actions will also be referred to the ASF board by the PMC chair. | Consensus | Active PMC members (excluding the committer in question if member of the PMC) | 7 Days | private@ |
| PMC Member Removal | When removal of a PMC member is sought. Note: Such actions will also be referred to the ASF board by the PMC chair. | Consensus | Active PMC members (excluding the member in question) | 7 Days | private@ |
| Modifying Bylaws | Modifying this document. | 2/3 Majority | Active PMC members | 7 Days | dev@ |

