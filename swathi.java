/*
 * Copyright 2021, Savvas Learning Company LLC
 *
 * RBSAssignmentDataFetcher.java
 */
package com.savvas.ltg.rbs.assignments.datafetchers;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.savvas.ltg.rbs.assignments.entity.assignments.AssignedTo;
import com.savvas.ltg.rbs.assignments.entity.assignments.AssignmentContentItem;
import com.savvas.ltg.rbs.assignments.entity.assignments.AssignmentCount;
import com.savvas.ltg.rbs.assignments.entity.assignments.AssignmentDetails;
import com.savvas.ltg.rbs.assignments.entity.assignments.AssignmentWithStudentScore;
import com.savvas.ltg.rbs.assignments.entity.assignments.ClassAssignment;
import com.savvas.ltg.rbs.assignments.entity.assignments.ClassAssignmentDetails;
import com.savvas.ltg.rbs.assignments.entity.assignments.ClassAssignmentsSummary;
import com.savvas.ltg.rbs.assignments.entity.assignments.ProgramHierarchyAndAssignedToDetails;
import com.savvas.ltg.rbs.assignments.entity.assignments.StudentClassAssignment;
import com.savvas.ltg.rbs.assignments.entity.assignments.AssignmentWithStudentScoreAndCount;
import com.savvas.ltg.rbs.assignments.entity.content.CompositeTableOfContentItem;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.server.ServerWebExchange;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.pearson.ltg.assignments.entity.Assignment;
import com.pearson.ltg.assignments.entity.AssignmentType;
import com.pearson.ltg.assignments.entity.CompletionStatus;
import com.pearson.ltg.assignments.entity.ItemType;
import com.pearson.ltg.assignments.entity.RequestFilter;
import com.pearson.ltg.assignments.entity.Role;
import com.pearson.ltg.assignments.entity.ScoreSource;
import com.pearson.ltg.assignments.entity.UserAssignment;
import com.pearson.ltg.assignments.entity.UserAssignmentData;
import com.pearson.ltg.assignments.entity.UserAssignmentLanguage;
import com.pearson.ltg.common.content.entity.ContentType;
import com.pearson.ltg.common.content.entity.FileType;
import com.pearson.ltg.common.content.entity.Identity;
import com.pearson.ltg.common.content.entity.MediaType;
import com.pearson.ltg.common.content.entity.Permission;
import com.pearson.ltg.common.content.entity.TableOfContentItem;
import com.savvas.ltg.rbs.assignments.entity.assignments.TransferAssignmentDetails;
import com.savvas.ltg.rbs.assignments.entity.TransferClassRosterDetails;
import com.pearson.roster.entity.Student;
import com.savvas.ltg.rbs.assignments.auth.AssignmentsAuthContext;
import com.savvas.ltg.rbs.assignments.clients.assessment.IRBSAssessmentServiceClient;
import com.savvas.ltg.rbs.assignments.clients.assignment.RBSAssignmentServiceClient;
import com.savvas.ltg.rbs.assignments.clients.css.IClassSyncServiceClient;
import com.savvas.ltg.rbs.assignments.clients.externalClassMapping.IExternalClassMappingService;
import com.savvas.ltg.rbs.assignments.clients.roster.IRosterServiceClient;
import com.savvas.ltg.rbs.assignments.clients.rta.IRealizeServiceClient;
import com.savvas.ltg.rbs.assignments.clients.userMapping.IUserMappingServiceClient;
import com.savvas.ltg.rbs.assignments.clients.userprofile.UserProfileServiceClient;
import com.savvas.ltg.rbs.assignments.constants.ServiceConstants;
import com.savvas.ltg.rbs.assignments.entity.BulkLinkedUsersRequest;
import com.savvas.ltg.rbs.assignments.entity.ClassRosterDetails;
import com.savvas.ltg.rbs.assignments.entity.GoogleLinkedUser;
import com.savvas.ltg.rbs.assignments.entity.GoogleLinkedUsers;
import com.savvas.ltg.rbs.assignments.entity.assessment.AssessmentRequest;
import com.savvas.ltg.rbs.assignments.entity.group.Group;
import com.savvas.ltg.rbs.assignments.entity.userprofile.RumbaUserAttribute;
import com.savvas.ltg.rbs.assignments.exceptions.BadDataException;
import com.savvas.ltg.rbs.assignments.exceptions.DataFetchingException;
import com.savvas.ltg.rbs.assignments.services.ContentService;
import com.savvas.ltg.rbs.assignments.services.ProxyUrlConverterService;
import com.savvas.ltg.rbs.assignments.services.RosterService;
import com.savvas.ltg.rbs.assignments.utilities.AssignmentUtils;
import com.savvas.ltg.rbs.auth.pojo.AuthScope;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class RBSAssignmentDataFetcher implements DataFetcher<CompletableFuture<Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RBSAssignmentDataFetcher.class);
    private static final String ITEM_SEQUENCE = ItemType.SEQUENCE.toString();
    public static final Double KNEWTON_CORRECT_SCORE = 100.00;

    @Autowired
    RBSAssignmentServiceClient assignmentServiceClient;

    @Autowired
    ProxyUrlConverterService proxyUrlConverterService;

    @Autowired
    ContentService contentService;

    @Autowired
    RosterService rosterService;

    @Autowired
    IRosterServiceClient rosterServiceClient;

    @Autowired
    AssignmentUtils assignmentUtils;

    @Autowired
    IRBSAssessmentServiceClient rbsAssessmentServiceClient;

    @Autowired
    IExternalClassMappingService externalClassMappingServiceClient;

    @Autowired
    IUserMappingServiceClient userMappingServiceClient;

    @Autowired
    IRealizeServiceClient realizeServiceClient;

    @Autowired
    IClassSyncServiceClient classSyncServiceClient;

    @Autowired
    UserProfileServiceClient userProfileServiceClient;


    @Value("${contentservice.eps.pearson.collectionUuid}")
    String pearsonCollectionUuid;

    @Value("${contentservice.eps.community.collectionUuid}")
    String communityCollectionUuid;

    @Value("${contentservice.eps.assignment.collectionUuid}")
    String assignmentCollectionUuid;

    @Value("${rumba.user.role.permissions}")
    String roleToPermissions;

    public CompletableFuture<AssignmentDetails> getAssignmentDetails(DataFetchingEnvironment environment) throws DataFetchingException {

        String classId = environment.getArgument(ServiceConstants.CLASS_ID);
        String assignmentId = environment.getArgument(ServiceConstants.ASSIGNMENT_ID);
        AssignmentsAuthContext authContext = environment.getContext();
        Boolean excludeStudentGoogleLinks = Objects.nonNull(environment.getArgument(ServiceConstants.EXCLUDESTUDENTGOOGLELINKS))? environment.getArgument(ServiceConstants.EXCLUDESTUDENTGOOGLELINKS): false;
        AuthScope authScope = authContext.getAuthScope();
        String userId = authScope.getUserId();
        assignmentUtils.hasUserAccessOverClass(authScope.getUserId(), classId);
        List<AssignmentDetails> assignmentList = new ArrayList<>();
        LOGGER.debug("Data fetcher for loading the details of assignment for classId {} and assignmentId {}",
                classId, assignmentId);
        Mono<AssignmentDetails> response = rosterService.getSectionDetails(classId, userId)
                .map(classRosterDetails -> {
                    return classRosterDetails;
                    })
                .zipWhen((classRosterDetails) -> {
                    List<String> googleLinkedUserIds = new ArrayList<String>();
                    if(excludeStudentGoogleLinks == false) {
                         googleLinkedUserIds = classRosterDetails.getCmsClass().getData().getSection().getData().getSectionInfo().getStudents().stream()
                        .map(Student::getStudentPiId).collect(Collectors.toList());
                    }
                    googleLinkedUserIds.add(userId);
                    Mono<AssignmentDetails> assignmentDetailsMono = Mono.zip(assignmentServiceClient.getAssignment(classRosterDetails.getCmsClass()
                                    .getData().getSection().getId(), assignmentId, com.savvas.ltg.rbs.assignments.entity.Role.TEACHER.toString(),
                            userId), getExternalClassMapping(classRosterDetails, classId, googleLinkedUserIds)).flatMap(t2 -> {
                                GoogleLinkedUsers googleLinkedUsers= t2.getT2();
                                if (Objects.nonNull(googleLinkedUsers) && Objects.nonNull(googleLinkedUsers.getData())) {
                                    // verifying the teacher is linked
                                    classRosterDetails.setIsLinkedClass(BooleanUtils.isTrue(googleLinkedUsers.getData()
                                            .getOrDefault(userId, new GoogleLinkedUser()).getLinked()));
                                    classRosterDetails.setGoogleLinkedUsers(googleLinkedUsers.getData().values());
                                } else {
                                    classRosterDetails.setIsLinkedClass(false);
                                }

                                return Mono.just(t2.getT1());
                            });
                    return assignmentDetailsMono.flatMap(assignment -> {

                        LOGGER.debug("Got Assignment response for classId {} and assignmentId {}",
                                classId, assignmentId);

                        AssignmentDetails assignmentInfo = new AssignmentDetails();
                        List<AssignmentDetails> assignmentDetailsList = new ArrayList<>();
                        if (Objects.nonNull(assignment)) {

                            LOGGER.debug("Got the response for assignments for classId {} and assignmentId {}",
                                    classId, assignmentId);

                            assignmentInfo = AssignmentUtils.filterAssignmentByDeletedStudents(assignment, classRosterDetails.getCmsClass()
                                    .getData().getSection().getData()
                                    .getSectionInfo().getStudents().stream()
                                    .map(Student::getStudentPiId).collect(Collectors.toList()));
                            assignmentInfo.setClassRosterDetails(classRosterDetails);
                            assignmentDetailsList.add(assignmentInfo);
                            if (requiresPseudoLesson(assignmentInfo.getType())) {
                                LOGGER.debug("Data fetcher for loading the details of assignment for pseudo lesson classId {} and assignmentId {}",
                                        classId, assignmentId);
                                Mono<CompositeTableOfContentItem> compositeTableOfContentItemMono = getContentItemFromAssignmentForMultiResourcesAssignment(assignmentInfo, authContext);
                                return compositeTableOfContentItemMono.flatMap(compositeTableOfContentItem -> {
                                    populateAverageScore(assignmentDetailsList.get(ServiceConstants.INITIAL_INDEX), authScope, compositeTableOfContentItem, environment);
                                    assignmentList.add(assignmentDetailsList.get(ServiceConstants.INITIAL_INDEX));
                                    return Mono.just(assignmentDetailsList.get(ServiceConstants.INITIAL_INDEX));
                                });
                            } else {
                                Identity identity = getCurrentIdentity(authScope);
                                return contentService.getContentItem(assignmentInfo.getItemUuid(), String.valueOf(assignmentInfo.getItemVersion()),
                                        identity, assignmentInfo.isHasMultipleLanguage(), authContext).flatMap(item -> {
                                    LOGGER.debug("Got the content details -1 for classId {} and assignmentId {}, itemId : {}",
                                            classId, assignmentId, item.getId());
                                    CompositeTableOfContentItem compositeTableOfContentItem = new CompositeTableOfContentItem();
                                    BeanUtils.copyProperties(item, compositeTableOfContentItem);
                                    AssignmentDetails otherAssignmentInfo = assignmentDetailsList.get(ServiceConstants.INITIAL_INDEX);
                                    return removeNewlyAddedContentItemFromAssignment(otherAssignmentInfo, compositeTableOfContentItem, identity,
                                            otherAssignmentInfo.isAllowMultipleLanguage(), authContext).flatMap(updatedContentItem->  {
                                        LOGGER.debug("Got the content details -2 for classId {} and assignmentId {}, itemId : {}",
                                                classId, assignmentId, item.getId());
                                        Mono<AssignmentDetails> otherAssignmentsData = getCompositeOtherAssignmentInfo(assignmentList, otherAssignmentInfo, classId, assignmentId, item.getId(), compositeTableOfContentItem, environment);
                                        return otherAssignmentsData;
                                    });
                                });
                            }
                        } else {
                            throw new BadDataException("Error: No assignment data Found");
                        }
                    });
                })
                .map(result -> { return result.getT2();}).onErrorReturn(new AssignmentDetails());
                return response.toFuture();
    }

    public CompletableFuture<TransferAssignmentDetails> getTransferAssignmentDetails(DataFetchingEnvironment environment) throws DataFetchingException {

        String classId = environment.getArgument(ServiceConstants.CLASS_ID);
        String activeClassId = environment.getArgument(ServiceConstants.ACTIVE_CLASS_ID);
        String assignmentId = environment.getArgument(ServiceConstants.ASSIGNMENT_ID);
        AssignmentsAuthContext authContext = environment.getContext();
        AuthScope authScope = authContext.getAuthScope();
        String userId = authScope.getUserId();
        //TODO Need to do checks for security fix.
        List<AssignmentDetails> assignmentList = new ArrayList<>();
        TransferClassRosterDetails transferClassRosterDetails = new TransferClassRosterDetails();
        LOGGER.debug("Data fetcher for loading the details of transfer assignment for classId {} and assignmentId {}",
                classId, assignmentId);
        Mono<TransferAssignmentDetails> response = rosterService.getSectionDetailsProxy(activeClassId, userId, ServiceConstants.YES, ServiceConstants.PLATFORM_REALIZE, authContext)
                .map(classRosterDetails -> classRosterDetails)
                .zipWhen((classRosterDetails -> assignmentServiceClient.getAssignment(classId, assignmentId, com.savvas.ltg.rbs.assignments.entity.Role.TEACHER.toString(), userId)
                        .flatMap(assignment -> {
                            LOGGER.debug("Got Transfer Assignment response for classId {} and assignmentId {}",
                                    classId, assignmentId);

                            BeanUtils.copyProperties(classRosterDetails, transferClassRosterDetails);
                            AssignmentDetails assignmentInfo = new AssignmentDetails();
                            List<AssignmentDetails> assignmentDetailsList = new ArrayList<>();
                            if (Objects.nonNull(assignment)) {

                                LOGGER.debug("Got the response for transfer assignments for classId {} and assignmentId {}",
                                        classId, assignmentId);

                                assignmentInfo = assignment;
                                assignmentDetailsList.add(assignmentInfo);
                                if (requiresPseudoLesson(assignmentInfo.getType())) {
                                    LOGGER.debug("Data fetcher for loading the details of transfer assignment for pseudo lesson classId {} and assignmentId {}",
                                            classId, assignmentId);
                                    Mono<CompositeTableOfContentItem> compositeTableOfContentItemMono = getContentItemFromAssignmentForMultiResourcesAssignment(assignmentInfo, authContext);
                                    return compositeTableOfContentItemMono.flatMap(compositeTableOfContentItem -> {
                                        populateAverageScore(assignmentDetailsList.get(ServiceConstants.INITIAL_INDEX), authScope, compositeTableOfContentItem, environment);
                                        assignmentList.add(assignmentDetailsList.get(ServiceConstants.INITIAL_INDEX));
                                        return Mono.just(assignmentDetailsList.get(ServiceConstants.INITIAL_INDEX));
                                    });
                                } else {
                                    Identity identity = getCurrentIdentity(authScope);
                                    return contentService.getContentItem(assignmentInfo.getItemUuid(), String.valueOf(assignmentInfo.getItemVersion()),
                                            identity, assignmentInfo.isHasMultipleLanguage(), authContext).flatMap(item -> {
                                        LOGGER.debug("Got the content details -1 for classId {} and transfer assignmentId {}, itemId : {}",
                                                classId, assignmentId, item.getId());
                                        CompositeTableOfContentItem compositeTableOfContentItem = new CompositeTableOfContentItem();
                                        BeanUtils.copyProperties(item, compositeTableOfContentItem);
                                        AssignmentDetails otherAssignmentInfo = assignmentDetailsList.get(ServiceConstants.INITIAL_INDEX);
                                        return removeNewlyAddedContentItemFromAssignment(otherAssignmentInfo, compositeTableOfContentItem, identity,
                                                otherAssignmentInfo.isAllowMultipleLanguage(), authContext).flatMap(updatedContentItem -> {
                                            LOGGER.debug("Got the content details -2 for classId {} and transfer assignmentId {}, itemId : {}",
                                                    classId, assignmentId, item.getId());
                                            Mono<AssignmentDetails> otherAssignmentsData = getCompositeOtherAssignmentInfo(assignmentList, otherAssignmentInfo, classId, assignmentId, item.getId(), compositeTableOfContentItem, environment);
                                            return otherAssignmentsData;
                                        });
                                    });
                                }
                            } else {
                                throw new BadDataException("Error: No transfer assignment data Found");
                            }
                        })))
                .map(result -> {
                    AssignmentDetails updatedAssignmentDetails = result.getT2();
                    TransferAssignmentDetails updatedTransferAssignmentInfo = new TransferAssignmentDetails();
                    updatedTransferAssignmentInfo.setTransferClassRosterDetails(transferClassRosterDetails);
                    BeanUtils.copyProperties(updatedAssignmentDetails, updatedTransferAssignmentInfo);
                    return updatedTransferAssignmentInfo;
                }).onErrorReturn(new TransferAssignmentDetails());
        return response.toFuture();
    }

    private Mono<GoogleLinkedUsers> getExternalClassMapping(ClassRosterDetails classRosterDetails, String classId, List<String> userIds) {
        if(StringUtils.compare(classRosterDetails.getCmsClass().getData().getSection().getData().getSectionInfo().getRosterSource(), ServiceConstants.ROSTER_SOURCE_GOOGLE) == 0) {
            return getGoogleLinkedUsers(userIds);
        }
        return externalClassMappingServiceClient.getExternalClassMappingExist(classId).flatMap(isLinkedClass-> {
            if (!isLinkedClass) {
                return Mono.just(new GoogleLinkedUsers());
            }
            return getGoogleLinkedUsers(userIds);
        });
    }

    private Mono<GoogleLinkedUsers> getGoogleLinkedUsers(List<String> userIds) {
        return classSyncServiceClient.getGoogleLinkedUsers(new BulkLinkedUsersRequest(userIds,
                ServiceConstants.EXTERNAL_SOURCE_GOOGLE, true));

    }

    public CompletableFuture<Boolean> changeAssignmentsVisibility(DataFetchingEnvironment environment)
            throws DataFetchingException {
        String classId = environment.getArgument(ServiceConstants.CLASS_ID);
        List<String> assignmentIds = environment.getArgument(ServiceConstants.ASSIGNMENT_IDS);
        Set<String> assignmentIdsSet = Set.copyOf(assignmentIds);
        String status = environment.getArgument(ServiceConstants.STATUS);
        boolean googleImportedClass = environment.getArgument(ServiceConstants.GOOGLE_IMPORTED_CLASS);

        if(CollectionUtils.isEmpty(assignmentIdsSet) || assignmentIdsSet.size() > 10) {
            throw new BadDataException("Provided assignments is not valid");
        }

        AssignmentsAuthContext authContext = environment.getContext();
        AuthScope authScope = authContext.getAuthScope();
        String userId = authScope.getUserId();

        LOGGER.debug("Getting Groups for userId :{}", userId, environment);

        return realizeServiceClient.updateAssignmentsStatus(userId, classId, assignmentIdsSet, status, googleImportedClass)
                .toFuture();
    }

    private Mono<CompositeTableOfContentItem> removeNewlyAddedContentItemFromAssignment(AssignmentDetails assignment, CompositeTableOfContentItem assignmentItem,
                                                                                        Identity identity, boolean multiLangAssignment, AssignmentsAuthContext authContext) {
        LOGGER.debug("Inside removeNewlyAddedContenItemFromAssignment assignmentId {}", assignment.getAssignmentId());
        Set<AssignmentContentItem> assignmentContentItems = new HashSet<>();
        if (assignmentItem.isSequence() || assignmentItem.isLesson()) {
            //Creating an object of all the item_uuids for all content items for which assignment was created
            for (UserAssignment userAssignment : assignment.getStudentMetadata()) {
                //excluding RRSCO type of item ids
                if (!userAssignment.isItemTypeRRSSCO()) {
                    assignmentContentItems.addAll(userAssignment.getUserAssignmentLanguageList().stream().filter(
                            userAssignmentLang -> userAssignmentLang.getIsSelected()).map(mapAssignmentContentItem).collect(Collectors.toList()));
                }
            }
            if (CollectionUtils.isNotEmpty(assignmentContentItems)) {
                String id = assignmentItem.getId();
                assignmentContentItems.removeIf(item -> StringUtils.equals(item.getItemUuid(), id));
                // filtering the child content  items based on the assignment item-uuids
                Set<String> assignmentIdsWhenAssigned = assignmentContentItems.stream().map(AssignmentContentItem::getItemUuid).collect(Collectors.toSet());
                Set<String> currentItemIds = getCurrentLeafAssignmentItems(assignmentItem);
                if (CollectionUtils.isNotEmpty(removeCommonItemIdsAddDifference(assignmentIdsWhenAssigned, currentItemIds, multiLangAssignment))) {
                    //If there are items added or removed after the assignment has been created we need to filter/add the contentItems
                    return addRemoveContentItemsInLesson(assignmentContentItems, assignmentItem, currentItemIds, identity, multiLangAssignment, authContext);
                }
            }
        }
        return Mono.just(assignmentItem);
    }

    Function<UserAssignmentLanguage, AssignmentContentItem> mapAssignmentContentItem = new Function<UserAssignmentLanguage, AssignmentContentItem>() {
        @Override
        public AssignmentContentItem apply(UserAssignmentLanguage userAssignmentLanguage) {
            AssignmentContentItem assignmentContentItem = new AssignmentContentItem();
            assignmentContentItem.setItemUuid(userAssignmentLanguage.getItemUuid());
            assignmentContentItem.setItemVersion(userAssignmentLanguage.getItemVersion());
            return assignmentContentItem;
        }
    };

    private Mono<CompositeTableOfContentItem> addRemoveContentItemsInLesson(
            Set<AssignmentContentItem> assignmentContentItems, CompositeTableOfContentItem assignmentItem,
            Set<String> currentItemIds, Identity identity, boolean getOtherLanguagesFromAssignment, AssignmentsAuthContext authContext) {
        LOGGER.debug(
                "Inside addRemoveContentItemsInLesson assignmentContentItems {}, currentItemIds : {}, multiLangAssignment : {}",
                assignmentContentItems, currentItemIds, getOtherLanguagesFromAssignment);
        Set<String> assignmentIdsWhenAssigned = assignmentContentItems.stream().map(AssignmentContentItem::getItemUuid)
                .collect(Collectors.toSet());
        return addRemoveContentItems(assignmentIdsWhenAssigned, assignmentItem, assignmentContentItems, identity,
                getOtherLanguagesFromAssignment, authContext).flatMap(contentChildrenItem -> {
                    CompositeTableOfContentItem assignmentContentItem = assignmentItem;
                    if (assignmentItem instanceof CompositeTableOfContentItem) {
                        assignmentContentItem.setContentItems(contentChildrenItem);
                    }
                    return Mono.just(assignmentContentItem);
                });
    }

    private Set<String> getCurrentLeafAssignmentItems(CompositeTableOfContentItem assignmentItem) {
        Set<String> allLeafContentIds = Sets.newHashSet();
        if(CollectionUtils.isNotEmpty(assignmentItem.getContentItems())) {
            for (TableOfContentItem contentItem :  assignmentItem.getContentItems()) {
                if (MediaType.Learning_Model.getKey().equals(contentItem.getMediaType())) {
                    if(CollectionUtils.isNotEmpty(contentItem.getContentItems())) {
                        allLeafContentIds.addAll(contentItem.getContentItems().stream()
                                .map(TableOfContentItem::getId).collect(Collectors.toSet()));
                    }
                } else {
                    allLeafContentIds.add(contentItem.getId());
                }
            }
        }
        return allLeafContentIds;
    }

    // This method is used to remove the union of the assignment Ids and current leaf itemids
    private List<String> removeCommonItemIdsAddDifference (Set<String> assignmentIdsWhenAssigned, Set<String> currentItemIds, boolean multiLangAssignment) {
        LOGGER.debug(
                "Inside removeCommonItemIdsAddDifference assignmentIdsWhenAssigned {}, currentItemIds : {}, multiLangAssignment : {}",
                assignmentIdsWhenAssigned, currentItemIds, multiLangAssignment);
        //We do not want to get the union of items if it is multi-language assignment because assignmentId and selected (language) itemids could be
        // different and it will get the item from other language which is not required.
        if (assignmentIdsWhenAssigned.size() == currentItemIds.size() && multiLangAssignment) {
            return Collections.emptyList();
        }
        List<String> union = Lists.newArrayList(assignmentIdsWhenAssigned);
        union.addAll(currentItemIds);
        // Prepare an intersection
        List<String> intersection = Lists.newArrayList(assignmentIdsWhenAssigned);
        intersection.retainAll(currentItemIds);
        // Subtract the intersection from the union
        union.removeAll(intersection);
        return union;
    }

    private Mono<List<TableOfContentItem>> addRemoveContentItems(Set<String> assignmentIds, TableOfContentItem assignmentItem, Set<AssignmentContentItem> assignedContentItems,
                                                                 Identity identity, boolean getOtherLanguagesFromAssignment, AssignmentsAuthContext authContext) {
        LOGGER.debug(
                "Inside addRemoveContentItems assignmentIds {}, currentItemIds : {}, assignedContentItems : {}, multiLangAssignment : {}",
                assignmentIds, assignmentItem, assignedContentItems, getOtherLanguagesFromAssignment);
        List<TableOfContentItem> childContentItems = Lists.newArrayList();
        if(CollectionUtils.isNotEmpty(assignmentItem.getContentItems())) {
            for (TableOfContentItem contentItem : assignmentItem.getContentItems()) {
                if (MediaType.Learning_Model.getKey().equals(contentItem.getMediaType())) {
                    handleLearningModelsInLessonAssignment (contentItem, assignmentIds, assignedContentItems, childContentItems);
                } else if (assignmentIds.contains(contentItem.getId())) {
                    childContentItems.add(contentItem);
                    assignedContentItems.removeIf(item -> item.getItemUuid().equals(contentItem.getId()));
                }
            }
        }
        // Once all the newly items have been removed from the hierarchy then we need to add any item which were there while assignment was created
        if (CollectionUtils.isNotEmpty(assignedContentItems)) {
            return getAdditionalContentItemsForAssignmentContentItem(assignedContentItems,
                    identity, getOtherLanguagesFromAssignment, authContext).flatMap(data -> {
                        childContentItems.addAll(data);
                        return Mono.just(childContentItems);
                    });

        }
        return Mono.just(childContentItems);
    }

    private void handleLearningModelsInLessonAssignment (TableOfContentItem contentItem, Set<String> assignmentIds,
                                                         Set<AssignmentContentItem> assignedContentItems, List<TableOfContentItem> childContentItems) {

        LOGGER.debug(
                "Inside handleLearningModelsInLessonAssignment contentItem {}, assignmentIds : {}, assignedContentItems : {}, childContentItems : {}",
                contentItem, assignmentIds, assignedContentItems, childContentItems);

        List<TableOfContentItem> learningModelChildContentItems = Lists.newArrayList();
        //Removing any content item from learning model if the item is newly added in learning model hierarchy
        if(CollectionUtils.isNotEmpty(contentItem.getContentItems())) {
            for (TableOfContentItem lmChildContentItem : contentItem.getContentItems()) {
                if (assignmentIds.contains(lmChildContentItem.getId())) {
                    assignedContentItems.removeIf(item -> item.getItemUuid().equals(lmChildContentItem.getId()));
                    learningModelChildContentItems.add(lmChildContentItem);
                }
            }
        }
        if(CollectionUtils.isNotEmpty(learningModelChildContentItems)) {
            if (contentItem instanceof CompositeTableOfContentItem) {
                CompositeTableOfContentItem learningModelContentItem = (CompositeTableOfContentItem) contentItem;
                learningModelContentItem.setContentItems(learningModelChildContentItems);
            }
            childContentItems.add(contentItem);
        }
    }

    private Mono<List<TableOfContentItem>> getAdditionalContentItemsForAssignmentContentItem(
            Set<AssignmentContentItem> assignmentContentsToAdd, Identity identity,
            boolean getOtherLanguagesFromAssignment, AssignmentsAuthContext authContext) {
        LOGGER.debug(
                "Inside getAdditionalContentItemsForAssignmentContentItem assignmentContentsToAdd {}, multiLangAssignment : {}",
                assignmentContentsToAdd, getOtherLanguagesFromAssignment);

        return Flux.fromIterable(assignmentContentsToAdd)
                .flatMap(assignmentContentItem -> contentService.getContentItem(assignmentContentItem.getItemUuid(),
                        String.valueOf(assignmentContentItem.getItemVersion()), identity,
                        getOtherLanguagesFromAssignment, authContext))
                .collectList();
    }

    private void populateAverageScore (AssignmentDetails assignmentDetails, AuthScope authScope,
                                       CompositeTableOfContentItem compositeTableOfContentItem, DataFetchingEnvironment environment) {
        assignmentDetails.setContentItem(compositeTableOfContentItem);

        calculateAssignmentAverageForInProgressAndNotStartedStudents(assignmentDetails);
        updateAssignmentAttachmentDownloadUrls(assignmentDetails, authScope);
        if (isTestNavAssignment(compositeTableOfContentItem)) {
            populateEssayScoringAttribute(assignmentDetails, environment);
        }

        if (isAdaptiveAssignment(assignmentDetails) && hasRole(authScope, Role.TEACHER.toString())) {
            List<UserAssignment> summarizedStudentMetaData = summaryAdaptiveTaskAssignment(assignmentDetails.getStudentMetadata());
            assignmentDetails.setStudentMetadata(summarizedStudentMetaData);
        }
        adaptiveAssignmentsSummaryTeacherView(assignmentDetails, authScope);
        fillAverageScore(assignmentDetails);
    }

    private void adaptiveAssignmentsSummaryTeacherView(AssignmentDetails assignmentDetails, AuthScope authScope) {
        if (Boolean.TRUE.equals(isAdaptiveAssignment(assignmentDetails))
                && hasRole(authScope, Role.TEACHER.toString())) {
            List<UserAssignment> summarizedStudentMetaData = summaryAdaptiveTaskAssignment(
                    assignmentDetails.getStudentMetadata());
            assignmentDetails.setStudentMetadata(summarizedStudentMetaData);
        }
    }

    private Boolean isAdaptiveAssignment(AssignmentDetails assignmentDetails){
        return assignmentDetails.getType().equals(AssignmentType.ADAPTIVE);
    }

    private Boolean isMultistageAssignment(AssignmentDetails assignmentDetails){
        return assignmentDetails.getType() == AssignmentType.MULTISTAGE;
    }

    private List<UserAssignment> summaryAdaptiveTaskAssignment(List<UserAssignment> studentMetaData) {
        Collections.sort(studentMetaData, new UserAssignmentComparator());
        List<UserAssignment> summarizedStudentMetaData = new ArrayList<>();
        Map<String, UserAssignment> userAssignmentMap = new HashMap<>();
        for (UserAssignment userAssignment:studentMetaData) {
            if (userAssignment.isItemTypeSequence()) {
                summarizedStudentMetaData.add(userAssignment);
                userAssignmentMap.put(userAssignment.getStudentUuid(), userAssignment);
            }
            if (userAssignment.isItemTypeTest()) {
                UserAssignment adaptiveUserAssignment = userAssignmentMap.get(userAssignment.getStudentUuid());
                adaptiveUserAssignment.incrementAdaptiveTaskCount(checkAnswer(userAssignment));
            }
        }
        return summarizedStudentMetaData;
    }

    void fillAverageScore(AssignmentDetails assignment) {
        if (assignment.getContentItem().isLesson()) {
            populateLessonAssignmentInfo(assignment);
        }

        List<Double> scores = assignmentUtils.getScores(assignment);
        boolean includeNulls = false;
        assignment.setAverageScore(assignmentUtils.calculateAverage(scores, includeNulls));
    }

    private boolean isTestNavAssignment(CompositeTableOfContentItem assignmentItem) {
        if(assignmentItem == null || !assignmentItem.isTest()|| assignmentItem.getPlayerTarget()==null ) {
            return false;
        }
        String playerTarget = assignmentItem.getPlayerTarget();
        if(StringUtils.isEmpty(playerTarget) || !"testnav".equalsIgnoreCase(playerTarget)) {
            return false;
        }
        return true;
    }

    void populateLessonAssignmentInfo(AssignmentDetails assignment) {
        List<UserAssignment> userAssignments = assignment.getStudentMetadata();
        populateLessonAssignmentInfo(userAssignments, assignment.getAssignmentId());
    }

    private void populateLessonAssignmentInfo(List<UserAssignment> userAssignments, String assignmentId) {
        for (UserAssignment studentAssignment : userAssignments) {
            List<UserAssignment> userAssignmentsAssociatedWithStudentAssignment =
                    filterParentUserAssignmentListForStudent(userAssignments, studentAssignment.getStudentUuid(), assignmentId);
            if (assignmentUtils.isLessonUserAssignment(studentAssignment, assignmentId) &&
                    assignmentUtils.doesUserAssignmentsHaveUserAssignmentDataExcludingRRSSco(userAssignmentsAssociatedWithStudentAssignment)) {
                createUserAssignmentDataForLessonBasedOffChildrenAssignments(studentAssignment, userAssignmentsAssociatedWithStudentAssignment);
            }
        }
    }

    private void createUserAssignmentDataForLessonBasedOffChildrenAssignments(UserAssignment lessonAssignment, List<UserAssignment> associatedUserAssignments) {
        List<UserAssignmentData> userAssignmentDataList = new ArrayList<>();
        UserAssignmentData userAssignmentData = createUserAssignmentData(lessonAssignment
                .getUserAssignmentId());
        populateLessonAssignmentDataFromUserAssignments(associatedUserAssignments, userAssignmentData);
        userAssignmentData.setCompletionStatus(lessonAssignment.getStatus());
        userAssignmentDataList.add(userAssignmentData);
        lessonAssignment.setUserAssignmentDataList(userAssignmentDataList);
    }

    private UserAssignmentData createUserAssignmentData(String userAssignmentId) {
        UserAssignmentData userAssignmentData = new UserAssignmentData();
        userAssignmentData.setUserAssignmentId(userAssignmentId);

        return userAssignmentData;
    }

    private List<UserAssignment> filterParentUserAssignmentListForStudent(List<UserAssignment> userAssignments,
                                                                          String studentUuid, String parentAssignmentId) {
        List<UserAssignment> studentUserAssignments = new ArrayList<>();

        for (UserAssignment userAssignment : userAssignments) {
            if (StringUtils.equals(userAssignment.getStudentUuid(), studentUuid) &&
                    StringUtils.equals(userAssignment.getAssignmentId(), parentAssignmentId)) {
                studentUserAssignments.add(userAssignment);
            }
        }
        return studentUserAssignments;
    }

    private Boolean checkAnswer(UserAssignment taskUserAssignment) {
        UserAssignmentData userAssignmentData = taskUserAssignment.getLatestUserAssignmentData();
        if(null != userAssignmentData && null != userAssignmentData.getScore()) {
            return KNEWTON_CORRECT_SCORE.equals(userAssignmentData.getScore());
        }
        return false;
    }

    private void populateEssayScoringAttribute(AssignmentDetails assignment, DataFetchingEnvironment environment) {
        try {
            LOGGER.debug("Calling getAssessmentInfo for assignment {}, environment {}",
                    assignment, environment);
            AssignmentsAuthContext authContext = environment.getContext();
            ServerWebExchange webExchange = authContext.getServerWebExchange();
            AssessmentRequest request = new AssessmentRequest();
            request.setAuthToken(webExchange.getRequest().getHeaders().get(ServiceConstants.AUTHORIZATION).get(0));
            request.setUserId(webExchange.getRequest().getHeaders().get(ServiceConstants.USERID).get(0));
            request.setItemUuid(assignment.getItemUuid());
            request.setItemVersion(assignment.getItemVersion());
            request.setIncludeQuestions(Boolean.TRUE);
            rbsAssessmentServiceClient.getAssessmentInfo(request).map(assessmentInfo -> {
                assignment.setHasEssayScoring(assessmentInfo.isHasEssayScoring());
                if(assessmentInfo.isHasEssayScoring()) {
                    assignment.setMaxScore(assessmentInfo.getMaxScore());
                }
                return assignment;
            }).subscribe();
        } catch (Exception excep) {
            LOGGER.error("Exception while getting Assessment Info details", excep);
        }
    }

    private AssignmentDetails updateAssignmentAttachmentDownloadUrls(AssignmentDetails assignment, AuthScope authScope) {
        for(UserAssignment userAssignment : assignment.getStudentMetadata()) {
            String attachmentUrl = userAssignment.getAttachmentUrl();
            attachmentUrl = proxyUrlConverterService.convertProxyUrlToProxyDownloadUrl(attachmentUrl, authScope.getUserId());
            userAssignment.setAttachmentUrl(attachmentUrl);
        }
        return assignment;
    }

    /**
     * This method will set avg score for IN_Progress & NotStarted assignment
     * @param assignment
     */
    private void calculateAssignmentAverageForInProgressAndNotStartedStudents(AssignmentDetails assignment) {
        if (assignment.isPastDue()) {
            Double averageNotStartedScore =
                    getAverageScoreByCompletionStatusAndScoreSource(assignment.getStudentMetadata(), CompletionStatus.NOT_STARTED, ScoreSource.MANUAL);

            Double averageInProgressScore =
                    getAverageScoreByCompletionStatusAndScoreSource(assignment.getStudentMetadata(), CompletionStatus.IN_PROGRESS, ScoreSource.MANUAL);

            assignment.setAverageInProgressScore(averageInProgressScore);
            assignment.setAverageNotStartedScore(averageNotStartedScore);
        }
    }

    /**
     * Teacher enter manual generic score for all students who comes under NOT_STARTED / IN PROGRESS
     * category hence while calculation average score / student count to display on the screen.
     */
    private Double getAverageScoreByCompletionStatusAndScoreSource(
            List<UserAssignment> assignedStudentList,
            CompletionStatus status,
            ScoreSource scoreSource) {

        double totalScore = 0.0;
        int studentCount = 0;

        for (UserAssignment studentData : assignedStudentList) {

            if (studentData.getStatus().equals(status)) {
                UserAssignmentData userAssignmentData = studentData.getLatestUserAssignmentData();

                if (userAssignmentData != null && userAssignmentData.getScoreSource() == scoreSource && !userAssignmentData.getNeedsManualScoring()) {
                    totalScore += userAssignmentData.getScore();
                    studentCount += 1;
                }
            }
        }

        if (studentCount > 0) {
            return totalScore / studentCount;
        } else {
            return null;
        }
    }

    Mono<CompositeTableOfContentItem> getContentItemFromAssignmentForMultiResourcesAssignment(Assignment assignment, AssignmentsAuthContext authContext) {
        LOGGER.debug("Inside getContentItemFromAssignmentForMultiResourcesAssignment assignmentId {}",
                assignment.getAssignmentId());
        boolean getMultiLanguagesWithSearch = false;
        Collection<AssignmentContentItem> itemInfo = toCreateContentItemsForMultiResource(assignment, authContext.getAuthScope());
        Mono<CompositeTableOfContentItem> compositeTableOfContentItemLessonMono = getContentItemForMultiResourcesAssignment(assignment.getItemUuid(), itemInfo, getMultiLanguagesWithSearch, authContext);
        if (assignment.isHasMultipleLanguage()) {
            return compositeTableOfContentItemLessonMono.flatMap(contentItem -> populateOtherLanguagesFromAssignment(contentItem, assignment, assignment.isHasMultipleLanguage(), authContext));
        }
        return compositeTableOfContentItemLessonMono;
    }

    private boolean requiresPseudoLesson(AssignmentType assignmentType) {
        return AssignmentType.MULTIRESOURCE.equals(assignmentType) ||
                AssignmentType.PLAYLIST.equals(assignmentType);
    }

    private boolean hasRole(AuthScope authScope, String role) {
        return authScope.getAccessInfo().getRole().stream().filter(Objects::nonNull).anyMatch(s -> s.equalsIgnoreCase(role));
    }

    private Collection<AssignmentContentItem> toCreateContentItemsForMultiResource(Assignment assignment, AuthScope authScope) {
        LOGGER.debug("Inside  toCreateContentItemsForMultiResource assignmentId {}",
                assignment.getAssignmentId());
        Predicate<UserAssignmentLanguage> studentFilter = userAssignmentLanguage -> userAssignmentLanguage.getIsSelected();
        Predicate<UserAssignmentLanguage> teacherFilter = userAssignmentLanguage -> userAssignmentLanguage.getIsdefault();
        Predicate<UserAssignmentLanguage> userAssignmentLanguageFilter = hasRole(authScope, Role.STUDENT.toString()) ? studentFilter : teacherFilter;
        Set<AssignmentContentItem> assignmentContentItems = new LinkedHashSet<>();
        Set<String> processedItems = new HashSet<>();
        assignment.getStudentMetadata().forEach(userAssignment -> {
            if (!userAssignment.isItemTypeRRSSCO() && CollectionUtils.isNotEmpty(userAssignment.getUserAssignmentLanguageList())) {
                UserAssignmentLanguage ual = userAssignment.getUserAssignmentLanguageList()
                        .stream().filter(userAssignmentLanguageFilter).findFirst().orElse(null);
                if(Objects.isNull(ual)) {
                    if (!StringUtils.equals(assignment.getAssignmentId(), userAssignment.getAssignmentId())
                            && (userAssignment.isItemTypeSequence() || userAssignment.isItemTypeLesson() ||
                            userAssignment.isItemTypeMultistage())) {
                        ual = userAssignment.getUserAssignmentLanguageList().get(0);
                    }
                }
                updateUserAssignmentLanguage(ual, processedItems, assignmentContentItems);
            }
        });
        assignmentContentItems.removeIf(filter -> filter.getItemUuid().equalsIgnoreCase(assignment.getItemUuid()));
        return assignmentContentItems;
    }

    Mono<CompositeTableOfContentItem> getContentItemForMultiResourcesAssignment(String itemId, Collection<AssignmentContentItem> assignmentContentItems, boolean getMultipleLanguages, AssignmentsAuthContext authContext){
        LOGGER.debug("Inside getContentItemForMultiResourcesAssignment itemId : {}, assignmentContentItems : {}",
                itemId, assignmentContentItems);

        CompositeTableOfContentItem assignmentContentItem = new CompositeTableOfContentItem();
        assignmentContentItem.setId(itemId);
        assignmentContentItem.setVersion(1);
        assignmentContentItem.setContentType(ContentType.SEQUENCE.getKey());
        assignmentContentItem.setFileType(FileType.SEQUENCE.getKey());
        assignmentContentItem.setMediaType(MediaType.Lesson.getKey());
        Mono<Map<String, TableOfContentItem>> childItemsMono = getContentItemsForMultiResourceAssignment(assignmentContentItems, getMultipleLanguages, authContext);
        return childItemsMono.flatMap(childItems -> {
            List<TableOfContentItem> childItemList = new ArrayList<>();
            for (AssignmentContentItem contentItem : assignmentContentItems) {
                childItemList.add(childItems.get(contentItem.getItemUuid()));
            }
            assignmentContentItem.setContentItems(childItemList);
            return Mono.just(assignmentContentItem);
        });
    }

    private Mono<Map<String, TableOfContentItem>> getContentItemsForMultiResourceAssignment(
            Collection<AssignmentContentItem> assignmentContentItems, boolean otherLanguage, AssignmentsAuthContext authContext) {
        LOGGER.debug("getContentItemsForMultiResourceAssignment requested items : {}", assignmentContentItems);
        return Flux.fromIterable(assignmentContentItems)
                .flatMap(assignmentContentItem -> contentService.getContentItem(assignmentContentItem.getItemUuid(),
                        String.valueOf(assignmentContentItem.getItemVersion()), getCurrentIdentity(authContext.getAuthScope()),
                        otherLanguage, authContext).map(childItem -> childItem))
                .collectMap(item -> item.getId(), item -> item);
    }

    /**
     * Extract contentItem ids & versions from userAssignments in userAssignmentList (i.e., from userAssignment.userAssignmentLanguageList),
     * retrieve corresponding ContentItems, and store as "map of lists" where the map key is contentItem primaryLanguageItemId.
     *
     * @param assignment
     * @return
     */
    Mono<CompositeTableOfContentItem> populateOtherLanguagesFromAssignment(CompositeTableOfContentItem contentItem, Assignment assignment, boolean otherLanguage,
                                                                           AssignmentsAuthContext authContext) {
        LOGGER.debug("Inside populateOtherLanguagesFromAssignment contentItem : {}, assignmentId {}, otherLanguage : {}",
                contentItem, assignment.getAssignmentId(), otherLanguage);

        Map<String, UserAssignmentLanguage> itemIds = new HashMap<>();
        assignment.getStudentMetadata().stream().filter(studentMetadata -> !studentMetadata.isItemTypeRRSSCO())
                .forEach(userAssignment -> {
                    userAssignment.getUserAssignmentLanguageList().forEach(userAssignmentLanguage -> {
                        if (!itemIds.containsKey(userAssignmentLanguage.getItemUuid()) && !StringUtils
                                .equalsAnyIgnoreCase(assignment.getItemUuid(), userAssignmentLanguage.getItemUuid())) {
                            itemIds.put(userAssignmentLanguage.getItemUuid(), userAssignmentLanguage);
                        }
                    });
                });

        Map<String, List<TableOfContentItem>> allContentItems = new HashMap<>();

        Mono<List<TableOfContentItem>> listMono = Flux.fromIterable(itemIds.values())
                .flatMap(userAssignmentLanguage -> contentService.getContentItem(userAssignmentLanguage.getItemUuid(),
                        String.valueOf(userAssignmentLanguage.getItemVersion()), getCurrentIdentity(authContext.getAuthScope()),
                        otherLanguage, authContext).flatMap(item -> Mono.just(item)))
                .collectList();

        return listMono.flatMap(tableOfContentItems -> {
            if (CollectionUtils.isNotEmpty(tableOfContentItems)) {
                tableOfContentItems.stream().filter(Objects::nonNull).forEach(tableOfContentItem -> {
                    populateContentItem(tableOfContentItem.getPrimaryLanguageItemId(), allContentItems,
                            tableOfContentItem);
                });
            }
            return populateOtherLanguages(contentItem, allContentItems).flatMap(compositeContentItem -> Mono.just(compositeContentItem));
        });
    }

    private void populateContentItem(String primaryLanguageItemId, Map<String, List<TableOfContentItem>> allContentItems1,
                                     TableOfContentItem contentItem) {
        LOGGER.debug("Inside populateContentItem primaryLanguageItemId : {}, allContentItems1 {}, contentItem : {}",
                primaryLanguageItemId, allContentItems1, contentItem);

        Map<String, List<TableOfContentItem>> allContentItems = allContentItems1;
        if (!allContentItems.containsKey(primaryLanguageItemId)) {
            allContentItems.put(primaryLanguageItemId, new ArrayList<TableOfContentItem>());
        }

        if (!allContentItems.get(primaryLanguageItemId).contains(contentItem)) {
            allContentItems.get(primaryLanguageItemId).add(contentItem);
        }
    }

    /**
     * For this contentItem, populate otherLanguages if corresponding items are found in possibleOtherLanguageItems based on
     * primaryLanguageItemId and language. Call recursively for child contentItems.
     *
     * @param contentItem
     * @param possibleOtherLanguageItems
     */
    Mono<CompositeTableOfContentItem> populateOtherLanguages(CompositeTableOfContentItem contentItem,
            Map<String, List<TableOfContentItem>> possibleOtherLanguageItems) {
        LOGGER.debug("Inside populateOtherLanguages contentItem : {}, possibleOtherLanguageItems : {}", contentItem,
                possibleOtherLanguageItems);
        String primaryLanguageId = contentItem.getPrimaryLanguageItemId();

        if (StringUtils.isNotEmpty(primaryLanguageId) && possibleOtherLanguageItems.containsKey(primaryLanguageId)) {
            Map<String, TableOfContentItem> otherLanguagesMap = new HashMap<>();
            String language = contentItem.getLanguage();

            for (TableOfContentItem otherLanguageItem : possibleOtherLanguageItems.get(primaryLanguageId)) {
                if (!StringUtils.equals(language, otherLanguageItem.getLanguage())) {
                    otherLanguagesMap.put(otherLanguageItem.getLanguage(), otherLanguageItem);
                }
            }
            contentItem.setOtherLanguages(otherLanguagesMap);
        }

        // recurse to child contentItems
        if (CollectionUtils.isNotEmpty(contentItem.getContentItems())) {
            for (TableOfContentItem childContentItem : contentItem.getContentItems()) {
                CompositeTableOfContentItem compositeTableOfContentItem = new CompositeTableOfContentItem();
                BeanUtils.copyProperties(childContentItem, compositeTableOfContentItem);
                populateOtherLanguages(compositeTableOfContentItem, possibleOtherLanguageItems);
            }
        }

        return Mono.just(contentItem);
    }

    protected void populateLessonAssignmentDataFromUserAssignments(List<UserAssignment> userAssignments, UserAssignmentData lessonAssignmentData) {
        Double totalScore = 0.0;

        if(isRRSManuallyScoredAfterLesson(userAssignments)) {
            Optional<UserAssignment> userAssignmentOption = userAssignments.stream()
                    .filter(userAssignment -> userAssignment.getUserAssignmentId()
                            .equals(lessonAssignmentData.getUserAssignmentId()))
                    .findFirst();
            if (userAssignmentOption.isPresent()) {
                userAssignmentOption.get().getUserAssignmentDataList().clear();
            }
        }

        int scorableItemCount = getScorableItemCount(userAssignments);
        boolean lessonAssignmentIsAlreadyScored = false;
        lessonAssignmentData.setScoreSent(true);
        lessonAssignmentData.setScore(0d);
        lessonAssignmentData.setScoreSource(ScoreSource.AUTOMATIC);

        for (UserAssignment studentAssignment : userAssignments) {

            UserAssignmentData studentAssignmentData = studentAssignment.getLatestUserAssignmentData();

            if ((studentAssignmentData != null && !studentAssignment.isItemTypeRRSSCO())) {
                if (studentAssignment.isItemTypeRealizeReaderSelection() && !isRRSItemMannuallyScored(studentAssignment)) {
                    continue;
                }
                if (hasLessonAssignmentBeenManuallyScored(studentAssignment, studentAssignmentData)) {
                    lessonAssignmentData.setScore(studentAssignmentData.getScore());
                    lessonAssignmentIsAlreadyScored = true;
                }
                if (studentAssignmentData.getScoreSource() == ScoreSource.MANUAL) {
                    totalScore = populatestudentAssignmentData(studentAssignmentData,lessonAssignmentData , totalScore, lessonAssignmentIsAlreadyScored);
                } else if (studentAssignmentData.getScore() != null) {
                    totalScore += studentAssignmentData.getScore();
                }
            }
        }
        if (!lessonAssignmentData.getNeedsManualScoring() && !lessonAssignmentIsAlreadyScored) {
            if (scorableItemCount > 0) { // Lesson with Scorable Items OR Mixed Lesson(Scorable + Non Scorable)
                lessonAssignmentData.setScore(totalScore / scorableItemCount);
            } else { // Lesson with Non Scorable Items (like PDF, PPT..)
                lessonAssignmentData.setScore(totalScore);
            }
        }
    }

    private boolean hasLessonAssignmentBeenManuallyScored(UserAssignment studentAssignment, UserAssignmentData assignmentData) {
        return (studentAssignment.isItemTypeLesson() || studentAssignment.isItemTypeSequence()) && !assignmentData.getNeedsManualScoring();
    }

    private double populatestudentAssignmentData(UserAssignmentData studentAssignmentData1 ,UserAssignmentData
            lessonAssignmentData1 , double totalScore, boolean lessonAssignmentIsAlreadyScored){
        UserAssignmentData studentAssignmentData = studentAssignmentData1;
        UserAssignmentData lessonAssignmentData   = lessonAssignmentData1;

        lessonAssignmentData.setScoreSource(ScoreSource.MANUAL);
        if (!studentAssignmentData.isScoreSent()) {
            lessonAssignmentData.setScoreSent(false);
        }
        if (!lessonAssignmentIsAlreadyScored) {
            if (studentAssignmentData.getScore() != null) {
                totalScore += studentAssignmentData.getScore();
            } else {
                lessonAssignmentData.setScore(null);
            }
        }
        return totalScore;
    }

    private boolean isRRSItemMannuallyScored(UserAssignment userAssignment) {
        return userAssignment.isItemTypeRealizeReaderSelection()
                && userAssignment.getLatestManualScore() != null
                && userAssignment.getLatestManualScore().getScore() != null;
    }

    private int getScorableItemCount(List<UserAssignment> userAssignments) {
        Set<String> uniqueItems = new HashSet<>();

        for (UserAssignment userAssignment : userAssignments) {
            // UserAssignment Must have at least 1 userAssignmentLanguage object by design
            String itemUuid = userAssignment.getUserAssignmentLanguageList().get(0).getItemUuid();
            if (userAssignment.isItemTypeSco() || userAssignment.isItemTypeTest() || userAssignment.isItemTypeTinCanSco()
                    || isRRSItemMannuallyScored(userAssignment)) {
                uniqueItems.add(itemUuid);
            } else {
                UserAssignmentData studentAssignmentData = userAssignment.getLatestUserAssignmentData();
                if (studentAssignmentData != null && studentAssignmentData.isScoreSent() && !userAssignment.isItemTypeRRSSCO()) {
                    uniqueItems.add(itemUuid);
                }
            }
        }
        return uniqueItems.size();
    }

    // To check if the RRS is manually scored after the lesson.
    private boolean isRRSManuallyScoredAfterLesson(List<UserAssignment> userAssignments) {
        UserAssignmentData lessonUserAssignmentData = null;
        Optional<UserAssignment> userAssignmentOption = userAssignments.stream().filter(t -> t.isItemTypeSequence()
                || t.isItemTypeLesson()).findFirst();
        // Check if the user lesson assignment is present.
        if (userAssignmentOption.isPresent()) {
            Optional<UserAssignment> lessonUserAssignmentOption =
                    userAssignments.stream().filter(userAssignment -> userAssignment.isItemTypeSequence()
                            || userAssignment.isItemTypeLesson()).findFirst();
            lessonUserAssignmentData = lessonUserAssignmentOption.isPresent() ?
                    lessonUserAssignmentOption.get().getLatestManualScore() : null;
        }
        // if lesson assignment never manually score , skip the next checks.
        if (lessonUserAssignmentData != null) {
            for (UserAssignment userAssignment : userAssignments) {
                if (userAssignment.isItemTypeRealizeReaderSelection()
                        && userAssignment.getLatestManualScore() != null
                        && userAssignment.getLatestManualScore().getCreateDate()
                        .isAfter(lessonUserAssignmentData.getCreateDate())) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public CompletableFuture<Object> get(DataFetchingEnvironment environment) throws Exception {
        throw new UnsupportedOperationException("Unsupported Operation Exception!");
    }

    public class UserAssignmentComparator implements Comparator<UserAssignment> {
        @Override
        public int compare(UserAssignment userAssignment1, UserAssignment userAssignment2) {
            String itemType1 = userAssignment1.getItemType();
            String itemType2 = userAssignment2.getItemType();
            if (ITEM_SEQUENCE.equalsIgnoreCase(itemType1) && ITEM_SEQUENCE.equalsIgnoreCase(itemType2)){
                return 0;
            }
            if (ITEM_SEQUENCE.equalsIgnoreCase(itemType1)){
                return -1;
            }
            if (ITEM_SEQUENCE.equalsIgnoreCase(itemType2)){
                return +1;
            }
            List<UserAssignmentData> userAssignmentDataList1 = userAssignment1.getUserAssignmentDataList();
            List<UserAssignmentData> userAssignmentDataList2 = userAssignment2.getUserAssignmentDataList();
            if (userAssignmentDataList1.isEmpty() && userAssignmentDataList2.isEmpty()) {
                return 0;
            }
            if (userAssignmentDataList1.isEmpty() && !userAssignmentDataList2.isEmpty()) {
                return +1;
            }
            if (!userAssignmentDataList1.isEmpty() && userAssignmentDataList2.isEmpty()) {
                return -1;
            }
            if (userAssignment1.getCreatedDate() != null && userAssignment2.getCreatedDate() != null) { 
                int dateCompareResult = userAssignment1.getCreatedDate().compareTo(userAssignment2.getCreatedDate());
                if (dateCompareResult != 0) {
                    return dateCompareResult;
                }
            }
            return userAssignment1.hashCode() - userAssignment2.hashCode();
        }
    }

    private Identity getCurrentIdentity(AuthScope authScope) {
        if(Objects.nonNull(authScope)) {
            return new Identity();
        }
        List<String> userRoles = authScope.getAccessInfo().getRole();
        String userId = authScope.getUserId();
        String userName = authScope.getAccessInfo().getUsername();
        String stateCode = authScope.getStateCode();

        Identity identity = new Identity();
        identity.setUserId(userId);
        identity.setUsername(userName);
        identity.setAppOfOrigin("TOC Viewer BFF");
        identity.setStateCode(stateCode);

        Map<String, List<String>> rolesToPermissionMap = new HashMap<>();
        Arrays.asList(roleToPermissions.split(ServiceConstants.PIPE_RGEX)).forEach(roleToPermission -> {
            String[] roleToPermissionArray = roleToPermission.split(ServiceConstants.COLON);
            rolesToPermissionMap.put(roleToPermissionArray[0], Arrays.asList(roleToPermissionArray[1].split(ServiceConstants.COMMA)));
        });

        HashSet<Permission> permissions = new HashSet<>();
        permissions.add(Permission.READ);
        userRoles.forEach(role -> {
            List<String> permissionList = rolesToPermissionMap.get(role);
            if(CollectionUtils.isNotEmpty(permissionList)) {
                permissionList.stream().parallel().forEach(p -> permissions.add(Permission.valueOf(p)));
            }
        });

        HashSet<String> collections = new HashSet<>();
        if (userRoles.contains(ServiceConstants.TEACHER)) {
            collections.add(pearsonCollectionUuid);
            collections.add(communityCollectionUuid);
        }
        collections.add(assignmentCollectionUuid);
        identity.setPermissions(permissions);
        identity.setCollections(collections);
        return identity;
    }

    public CompletableFuture<ClassAssignment> getAssignmentsByClassId(DataFetchingEnvironment environment) throws DataFetchingException {
        String classId = environment.getArgument(ServiceConstants.CLASS_ID);
        AssignmentsAuthContext authContext = environment.getContext();
        AuthScope authScope = authContext.getAuthScope();
        assignmentUtils.hasUserAccessOverClass(authScope.getUserId(), classId);
        String role;

        if (hasRole(authScope, com.savvas.ltg.rbs.assignments.entity.Role.PEARSON_ADMIN.toString())
                || hasRole(authScope, com.savvas.ltg.rbs.assignments.entity.Role.TEACHER.toString())) {
            role = com.savvas.ltg.rbs.assignments.entity.Role.TEACHER.toString();
        } else {
            role = com.savvas.ltg.rbs.assignments.entity.Role.STUDENT.toString();
        }

        MultiValueMap<String, String> queryParams = buildQueryParamsForAssignmentByClassId(environment);
        return assignmentServiceClient.getAssignmentsByClassId(classId, authScope.getUserId(), role, queryParams, authContext).toFuture();
    }

    public CompletableFuture<ClassAssignmentsSummary> getClassAssignmentsSummary(DataFetchingEnvironment environment) throws DataFetchingException {
        String classId = environment.getArgument(ServiceConstants.CLASS_ID);
        List<String> studentsNeeded = new ArrayList<>();

        AssignmentsAuthContext authContext = environment.getContext();
        AuthScope authScope = authContext.getAuthScope();
        assignmentUtils.hasUserAccessOverClass(authScope.getUserId(), classId);

        return rosterService.getSectionDetails(classId, authScope.getUserId()).map(classRosterDetails -> classRosterDetails)
                .zipWhen((classRosterDetails) -> {
                        String role;
                        MultiValueMap<String, String> queryParams = buildQueryParamsForAssignmentsSummaryByClassId(environment);
                        classRosterDetails.getCmsClass().getData().getSection().getData().getSectionInfo().getStudents().stream()
                                .forEach(student -> studentsNeeded.add(student.getStudentPiId()));
                        if (hasRole(authScope, com.savvas.ltg.rbs.assignments.entity.Role.PEARSON_ADMIN.toString())
                                || hasRole(authScope, com.savvas.ltg.rbs.assignments.entity.Role.TEACHER.toString())) {
                            role = com.savvas.ltg.rbs.assignments.entity.Role.TEACHER.toString();
                        } else {
                            role = com.savvas.ltg.rbs.assignments.entity.Role.STUDENT.toString();
                        }
                        return assignmentServiceClient.getClassAssignmentsSummary(classId, authScope.getUserId(), role, queryParams,
                                authContext).flatMap(classAssignmentsSummary -> {
                            classAssignmentsSummary.getClassAssignmentDetailsList().stream().forEach(classAssignmentDetails -> {
                                classAssignmentDetails.setAllStudents(new ArrayList<>(classAssignmentDetails.getAllStudents().stream()
                                        .filter(student -> studentsNeeded.contains(student))
                                        .collect(Collectors.toSet())));
                                classAssignmentDetails.setCompletedStudents(new ArrayList<>(classAssignmentDetails.getCompletedStudents().stream()
                                        .filter(student -> studentsNeeded.contains(student))
                                        .collect(Collectors.toSet())));
                            });
                            return Mono.just(classAssignmentsSummary);
                        });
                }).map(result -> result.getT2()).onErrorReturn(new ClassAssignmentsSummary()).toFuture();
    }

    public CompletableFuture<ClassAssignmentsSummary> getClassAssignmentsSummaryForV3(DataFetchingEnvironment environment) throws Exception {
        String classId = environment.getArgument(ServiceConstants.CLASS_ID);
        Boolean isDashBoard = Objects.nonNull(environment.getArgument(ServiceConstants.ISDASHBOARD))? environment.getArgument(ServiceConstants.ISDASHBOARD): false;
        List<String> studentsNeeded = new ArrayList<>();
        AssignmentsAuthContext authContext = environment.getContext();
        AuthScope authScope = authContext.getAuthScope();
        assignmentUtils.hasUserAccessOverClass(authScope.getUserId(), classId);
        Boolean isListing = Objects.nonNull(environment.getArgument(ServiceConstants.ISLISTING))? environment.getArgument(ServiceConstants.ISLISTING): false;
        return rosterService.getSectionDetails(classId, authScope.getUserId()).map(classRosterDetails -> classRosterDetails)
                .zipWhen((classRosterDetails) -> {
                        String role;
                        MultiValueMap<String, String> queryParams = buildQueryParamsForAssignmentsSummaryByClassId(environment);
                        classRosterDetails.getCmsClass().getData().getSection().getData().getSectionInfo().getStudents().stream()
                                .forEach(student -> studentsNeeded.add(student.getStudentPiId()));
                        if (hasRole(authScope, com.savvas.ltg.rbs.assignments.entity.Role.PEARSON_ADMIN.toString())
                                || hasRole(authScope, com.savvas.ltg.rbs.assignments.entity.Role.TEACHER.toString())) {
                            role = com.savvas.ltg.rbs.assignments.entity.Role.TEACHER.toString();
                        } else {
                            role = com.savvas.ltg.rbs.assignments.entity.Role.STUDENT.toString();
                        }
                        return assignmentServiceClient.getClassAssignmentsSummaryForV3(classId, authScope.getUserId(), role, queryParams,
                                authContext).flatMap(classAssignmentsSummary -> {
                            classAssignmentsSummary.getClassAssignmentDetailsList().stream().forEach(classAssignmentDetails -> {
                                classAssignmentDetails.setAllStudents(new ArrayList<>(classAssignmentDetails.getAllStudents().stream()
                                        .filter(student -> studentsNeeded.contains(student))
                                        .collect(Collectors.toSet())));
                                classAssignmentDetails.setCompletedStudents(new ArrayList<>(classAssignmentDetails.getCompletedStudents().stream()
                                        .filter(student -> studentsNeeded.contains(student))
                                        .collect(Collectors.toSet())));
                                if (isListing) {
                                    CompletableFuture<ClassAssignmentDetails> classAssignmentDetailsWithContentItem = getContentItemForClassAssignmentDetails(classAssignmentDetails, classId, authContext, authScope, environment).toFuture();
                                } else if (isDashBoard) {
                                    if(!requiresPseudoLesson(classAssignmentDetails.getAssignmentType())) {
                                        getContentItemForClassAssignmentDetails(classAssignmentDetails, classId, authContext, authScope, environment).toFuture();
                                    }
                                } else {
                                    filterAdaptiveAssignments(classAssignmentDetails, authScope);
                                }
                            });
                            return Mono.just(classAssignmentsSummary);
                        });
                }).map(result -> result.getT2()).onErrorReturn(new ClassAssignmentsSummary()).toFuture();
    }

    public CompletableFuture<Boolean> updateAssignmentScores(DataFetchingEnvironment environment) throws DataFetchingException {
        String classId = environment.getArgument(ServiceConstants.CLASS_ID);
        String assignmentId = environment.getArgument(ServiceConstants.ASSIGNMENT_ID);
        String userId = environment.getArgument(ServiceConstants.USERID);

        if(StringUtils.isBlank(classId)) {
            throw new BadDataException("Error: Invalid input value for classId");
        }
        if(StringUtils.isBlank(assignmentId)) {
            throw new BadDataException("Error: Invalid input value for assignmentId");
        }
        if(StringUtils.isBlank(userId)) {
            throw new BadDataException("Error: UserId cannot be null");
        }

        LOGGER.debug("Debug logs for RBSAssignmentDataFetcher updateAssignmentScores for classId {}, assignmentId{}, userId{} and environment {}",
                classId,assignmentId,userId, environment);
        Mono<Boolean> result = realizeServiceClient.updateAssignmentScores(userId, classId, assignmentId);
        return result.toFuture();
    }

    public CompletableFuture<Boolean> softDeleteUserAssignment(DataFetchingEnvironment environment) throws DataFetchingException {
        String userId = environment.getArgument(ServiceConstants.USERID);
        String userAssignmentId = environment.getArgument(ServiceConstants.USER_ASSIGNMENT_ID);

        if(StringUtils.isBlank(userAssignmentId)) {
            throw new BadDataException("Error: Invalid input value for userAssignmentId");
        }
        if(StringUtils.isBlank(userId)) {
            throw new BadDataException("Error: UserId cannot be null");
        }

        LOGGER.debug("Debug logs for RBSAssignmentDataFetcher softDeleteUserAssignment for userAssignmentId{}, userId{} and environment {}",
                userAssignmentId,userId, environment);
        return realizeServiceClient.softDeleteUserAssignment(userId, userAssignmentId).toFuture();
    }

    public CompletableFuture<Boolean> updateShowHideQuestionFlag(DataFetchingEnvironment environment) throws DataFetchingException {
        String userId = environment.getArgument(ServiceConstants.USERID);
        String assignmentId = environment.getArgument(ServiceConstants.ASSIGNMENT_ID);
        boolean enableViewQuestion = environment.getArgument(ServiceConstants.ENABLE_VIEW_QUESTION_FLAG);

        if(StringUtils.isBlank(assignmentId)) {
            throw new BadDataException("Error: Invalid input value for assignmentId or enableViewQuestion");
        }

        if(StringUtils.isBlank(userId)) {
            throw new BadDataException("Error: UserId cannot be null");
        }
        MultiValueMap<String, String> queryParams = new LinkedMultiValueMap<>();
        queryParams.add(ServiceConstants.ENABLE_VIEW_QUESTION_FLAG, String.valueOf(enableViewQuestion));

        LOGGER.debug("Debug logs for RBSAssignmentDataFetcher updateShowHideQuestionFlag for assignmentId{}, userId{} and environment {}",
                assignmentId, userId, environment);
        Mono<Boolean> result = assignmentServiceClient.updateShowHideQuestionFlag(userId, assignmentId, queryParams);
        return result.toFuture();
    }

    public CompletableFuture<AssignmentWithStudentScore> getAssignmentsByStudentIdAndClassId(DataFetchingEnvironment environment) throws DataFetchingException {
        String classId = environment.getArgument(ServiceConstants.CLASS_ID);
        String studentId = environment.getArgument(ServiceConstants.STUDENT_ID);
        AssignmentsAuthContext authContext = environment.getContext();
        AuthScope authScope = authContext.getAuthScope();
        assignmentUtils.hasUserAccessOverClass(authScope.getUserId(), classId);
        String role;

        if (hasRole(authScope, com.savvas.ltg.rbs.assignments.entity.Role.PEARSON_ADMIN.toString())
                || hasRole(authScope, com.savvas.ltg.rbs.assignments.entity.Role.TEACHER.toString())) {
            role = com.savvas.ltg.rbs.assignments.entity.Role.TEACHER.toString();
        } else {
            role = com.savvas.ltg.rbs.assignments.entity.Role.STUDENT.toString();
        }
        String searchPrefix = environment.getArgument(ServiceConstants.SEARCH_PREFIX);
        MultiValueMap<String, String> queryParams = buildQueryParamsForAssignmentByStudentIdAndClassId(environment);
        return assignmentServiceClient.getAssignmentsByStudentIdAndClassId(classId, studentId, searchPrefix, authScope.getUserId(), role, queryParams, authContext)
            .flatMap(assignmentWithStudentScore -> {
                Map<String, StudentClassAssignment> studentToAssignmentMappings = new HashMap<String, StudentClassAssignment>();
                studentToAssignmentMappings.put(studentId, new StudentClassAssignment(new ArrayList<UserAssignment>()));

                assignmentWithStudentScore.getClassAssignment().getAssignments().forEach(assignmentDetails -> {
                    if (isAdaptiveAssignment(assignmentDetails) || isMultistageAssignment(assignmentDetails)){
                        assignmentDetails.setStudentMetadata(filterSequenceUserAssignment(assignmentDetails.getStudentMetadata()));
                    }
                    if (assignmentDetails.containsLesson()) {
                        populateLessonAssignmentInfo(assignmentDetails);
                    }
                    if (StringUtils.equalsIgnoreCase(com.savvas.ltg.rbs.assignments.entity.Role.STUDENT.toString(),
                            role) || isContentItemExtractionNeeded(assignmentDetails.getStudentMetadata())) {
                        CompletableFuture<AssignmentDetails> assignmentDetailsWithContentItem = getContentItemForAssignmentDetails(
                                assignmentDetails, classId, authContext, authScope, environment).toFuture();
                    }
                    addUserAssignmentsToStudentSummaries(studentToAssignmentMappings, assignmentDetails);
                });
                assignmentWithStudentScore.setAssignmentAverage(studentToAssignmentMappings.get(studentId).getStudentAverageScore());
                assignmentWithStudentScore.setClassAssignment(assignmentWithStudentScore.getClassAssignment());
                return Mono.just(assignmentWithStudentScore);
            }).toFuture();        
    }

    public CompletableFuture<AssignmentWithStudentScoreAndCount> getAssignmentsByStudentIdAndClassIdV2(DataFetchingEnvironment environment) {
        String classId = environment.getArgument(ServiceConstants.CLASS_ID);
        String studentId = environment.getArgument(ServiceConstants.STUDENT_ID);
        Boolean includeAssignmentsCount = environment.getArgument(ServiceConstants.INCLUDE_ASSIGNMENT_COUNTS);
        AssignmentsAuthContext authContext = environment.getContext();
        AuthScope authScope = authContext.getAuthScope();
        assignmentUtils.hasUserAccessOverClass(authScope.getUserId(), classId);
        String role;

        if (hasRole(authScope, com.savvas.ltg.rbs.assignments.entity.Role.PEARSON_ADMIN.toString())
                || hasRole(authScope, com.savvas.ltg.rbs.assignments.entity.Role.TEACHER.toString())) {
            role = com.savvas.ltg.rbs.assignments.entity.Role.TEACHER.toString();
        } else {
            role = com.savvas.ltg.rbs.assignments.entity.Role.STUDENT.toString();
        }
        String searchPrefix = environment.getArgument(ServiceConstants.SEARCH_PREFIX);
        MultiValueMap<String, String> queryParams = buildQueryParamsForAssignmentByStudentIdAndClassId(environment);
        return assignmentServiceClient.getAssignmentsByStudentIdAndClassIdV2(classId, studentId, includeAssignmentsCount, searchPrefix, authScope.getUserId(), role, queryParams, authContext)
                .flatMap(assignmentWithStudentScore -> {
                    Map<String, StudentClassAssignment> studentToAssignmentMappings = new HashMap<String, StudentClassAssignment>();
                    studentToAssignmentMappings.put(studentId, new StudentClassAssignment(new ArrayList<UserAssignment>()));

                    assignmentWithStudentScore.getClassAssignment().getAssignments().forEach(assignmentDetails -> {
                        if (isAdaptiveAssignment(assignmentDetails) || isMultistageAssignment(assignmentDetails)) {
                            assignmentDetails.setStudentMetadata(filterSequenceUserAssignment(assignmentDetails.getStudentMetadata()));
                        }
                        if (assignmentDetails.containsLesson()) {
                            populateLessonAssignmentInfo(assignmentDetails);
                        }
                        if (StringUtils.equalsIgnoreCase(com.savvas.ltg.rbs.assignments.entity.Role.STUDENT.toString(),
                                role) || isContentItemExtractionNeeded(assignmentDetails.getStudentMetadata())) {
                            CompletableFuture<AssignmentDetails> assignmentDetailsWithContentItem = getContentItemForAssignmentDetails(
                                    assignmentDetails, classId, authContext, authScope, environment).toFuture();
                        }
                        addUserAssignmentsToStudentSummaries(studentToAssignmentMappings, assignmentDetails);
                    });
                    assignmentWithStudentScore.setAssignmentAverage(studentToAssignmentMappings.get(studentId).getStudentAverageScore());
                    assignmentWithStudentScore.setClassAssignment(assignmentWithStudentScore.getClassAssignment());
                    return Mono.just(assignmentWithStudentScore);
                }).toFuture();
    }

    public CompletableFuture<AssignmentCount> getAssignmentsCountByStudentIdAndClassId(
            DataFetchingEnvironment environment) throws DataFetchingException {
        String classId = environment.getArgument(ServiceConstants.CLASS_ID);
        String studentId = environment.getArgument(ServiceConstants.STUDENT_ID);
        if (StringUtils.isBlank(classId)) {
            throw new BadDataException("Error: Class Id is null or empty");
        }
        if (StringUtils.isBlank(studentId)) {
            throw new BadDataException("Error: Student Id is null or empty");
        }
        AssignmentsAuthContext authContext = environment.getContext();
        AuthScope authScope = authContext.getAuthScope();
        assignmentUtils.hasUserAccessOverClass(authScope.getUserId(), classId);
        String role;

        if (hasRole(authScope, com.savvas.ltg.rbs.assignments.entity.Role.PEARSON_ADMIN.toString())
                || hasRole(authScope, com.savvas.ltg.rbs.assignments.entity.Role.TEACHER.toString())) {
            role = com.savvas.ltg.rbs.assignments.entity.Role.TEACHER.toString();
        } else {
            role = com.savvas.ltg.rbs.assignments.entity.Role.STUDENT.toString();
        }
        return assignmentServiceClient
                .getAssignmentCountByStudentIdAndClassId(classId, authScope.getUserId(), studentId, role, authContext)
                .toFuture();
    }

    public CompletableFuture<AssignmentWithStudentScore> getAssignmentsForStudentIdWithMultiClassIds(DataFetchingEnvironment environment) throws DataFetchingException {
        List<String> classIds = environment.getArgument(ServiceConstants.CLASS_IDS);
        String studentId = environment.getArgument(ServiceConstants.STUDENT_ID);
        AssignmentsAuthContext authContext = environment.getContext();
        AuthScope authScope = authContext.getAuthScope();
        String userId = authScope.getUserId();
        String role = checkRole(authScope);
        //TODO Need to do checks for security fix.
        MultiValueMap<String, String> queryParams = buildQueryParamsForAssignmentByStudentIdAndClassId(environment);
        return assignmentServiceClient.getAssignmentsForStudentIdWithMultiClassIds(classIds, studentId, authScope.getUserId(), role, queryParams)
                .flatMap(classAssignments -> {
                    Map<String, StudentClassAssignment> studentToAssignmentMappings = new HashMap<String, StudentClassAssignment>();
                    studentToAssignmentMappings.put(studentId, new StudentClassAssignment(new ArrayList<UserAssignment>()));

                    classAssignments.getAssignments().forEach(assignmentDetails -> {
                        if (isAdaptiveAssignment(assignmentDetails) || isMultistageAssignment(assignmentDetails)){
                            assignmentDetails.setStudentMetadata(filterSequenceUserAssignment(assignmentDetails.getStudentMetadata()));
                        }
                        if (assignmentDetails.containsLesson()) {
                            populateLessonAssignmentInfo(assignmentDetails);
                        }
                        String assignmentClassId = assignmentDetails.getStudentMetadata().get(0).getClassUuid();
                        if (isContentItemExtractionNeeded(assignmentDetails.getStudentMetadata())) {
                            CompletableFuture<AssignmentDetails> assignmentDetailsWithContentItem = getContentItemForAssignmentDetails(assignmentDetails,
                                    assignmentClassId, authContext, authScope, environment).toFuture();
                        }
                        addUserAssignmentsToStudentSummaries(studentToAssignmentMappings, assignmentDetails);
                    });
                    AssignmentWithStudentScore assignmentWithStudentScore = new AssignmentWithStudentScore();
                    assignmentWithStudentScore.setAssignmentAverage(studentToAssignmentMappings.get(studentId).getStudentAverageScore());
                    assignmentWithStudentScore.setClassAssignment(classAssignments);
                    return Mono.just(assignmentWithStudentScore);
                }).toFuture();
    }

    private boolean isContentItemExtractionNeeded(List<UserAssignment> assignmentStudentMetaData) {
        boolean isContentItemsNeeded = false;
        if (assignmentStudentMetaData.size() > 0) {
            for(UserAssignment userAssignment: assignmentStudentMetaData) {
                if (StringUtils.equalsIgnoreCase(userAssignment.getItemType(), ItemType.TEST.toString())) {
                    isContentItemsNeeded = true;
                    break;
                }
            }
        }
        return isContentItemsNeeded;
    }

    private String checkRole(AuthScope authScope) {
        if (hasRole(authScope, com.savvas.ltg.rbs.assignments.entity.Role.PEARSON_ADMIN.toString())
                || hasRole(authScope, com.savvas.ltg.rbs.assignments.entity.Role.TEACHER.toString())) {
            return com.savvas.ltg.rbs.assignments.entity.Role.TEACHER.toString();
        } else {
            return com.savvas.ltg.rbs.assignments.entity.Role.STUDENT.toString();
        }
    }

    public Mono<AssignmentDetails> getContentItemForAssignmentDetails(AssignmentDetails assignmentDetails, String classId, AssignmentsAuthContext authContext,
                                                                      AuthScope authScope, DataFetchingEnvironment environment) {

        if (requiresPseudoLesson(assignmentDetails.getType())) {
            LOGGER.debug("Data fetcher for loading the details of assignment for pseudo lesson classId {} and assignmentId {}",
                    classId, assignmentDetails.getAssignmentId());
            Mono<CompositeTableOfContentItem> compositeTableOfContentItemMono = getContentItemFromAssignmentForMultiResourcesAssignment(assignmentDetails, authContext);
            return compositeTableOfContentItemMono.flatMap(compositeTableOfContentItem -> {
                populateAverageScore(assignmentDetails, authScope, compositeTableOfContentItem, environment);
                return Mono.just(assignmentDetails);
            });
        } else {
            Identity identity = getCurrentIdentity(authScope);
            return contentService.getContentItem(assignmentDetails.getItemUuid(), String.valueOf(assignmentDetails.getItemVersion()),
                    identity, assignmentDetails.isHasMultipleLanguage(), authContext).flatMap(item -> {
                LOGGER.debug("Got the content details -1 for classId {} and assignmentId {}, itemId : {}",
                        classId, assignmentDetails.getAssignmentId(), item.getId());
                CompositeTableOfContentItem compositeTableOfContentItem = new CompositeTableOfContentItem();
                BeanUtils.copyProperties(item, compositeTableOfContentItem);
                return removeNewlyAddedContentItemFromAssignment(assignmentDetails, compositeTableOfContentItem, identity,
                        assignmentDetails.isAllowMultipleLanguage(), authContext).flatMap(updatedContentItem->  {
                    LOGGER.debug("Got the content details -2 for classId {} and assignmentId {}, itemId : {}",
                            classId, assignmentDetails.getAssignmentId(), item.getId());
                    if (assignmentDetails.isHasMultipleLanguage()) {
                        LOGGER.debug("Got the content details -3 isHasMultipleLanguage for classId {} and assignmentId {}, itemId : {}",
                                classId, assignmentDetails.getAssignmentId(), item.getId());
                        Mono<CompositeTableOfContentItem> compositeTableOfContentItemMono = populateOtherLanguagesFromAssignment(compositeTableOfContentItem,
                                assignmentDetails, assignmentDetails.isHasMultipleLanguage(), authContext);
                        LOGGER.debug("Got the content details -4 isHasMultipleLanguage for classId {} and assignmentId {}, itemId : {}, compositeTableOfContentItemMono : {}",
                                classId, assignmentDetails.getAssignmentId(), item.getId(), compositeTableOfContentItemMono);
                        return compositeTableOfContentItemMono.flatMap(contentItem -> {
                            LOGGER.debug("Got the content details -5 isHasMultipleLanguage for classId {} and assignmentId {}, itemId : {}, other Lang : {}",
                                    classId, assignmentDetails.getAssignmentId(), item.getId(), contentItem.getId());
                            populateAverageScore(assignmentDetails, authScope, compositeTableOfContentItem, environment);
                            return Mono.just(assignmentDetails);
                        });
                    }
                    populateAverageScore(assignmentDetails, authScope, compositeTableOfContentItem, environment);
                    return Mono.just(assignmentDetails);
                });
            });
        }
    }

    public CompletableFuture<ClassAssignmentsSummary> searchAssignments(DataFetchingEnvironment environment) throws Exception {
        String classId = environment.getArgument(ServiceConstants.CLASS_ID);
        List<String> studentsNeeded = new ArrayList<>();
        AssignmentsAuthContext authContext = environment.getContext();
        AuthScope authScope = authContext.getAuthScope();
        assignmentUtils.hasUserAccessOverClass(authScope.getUserId(), classId);

        return rosterService.getSectionDetails(classId, authScope.getUserId()).map(classRosterDetails -> classRosterDetails)
                .zipWhen((classRosterDetails) -> {
                        String role;
                        MultiValueMap<String, String> queryParams = buildQueryParamsForSearchByClassId(environment);
                        classRosterDetails.getCmsClass().getData().getSection().getData().getSectionInfo().getStudents().stream()
                                .forEach(student -> studentsNeeded.add(student.getStudentPiId()));
                        if (hasRole(authScope, com.savvas.ltg.rbs.assignments.entity.Role.PEARSON_ADMIN.toString())
                                || hasRole(authScope, com.savvas.ltg.rbs.assignments.entity.Role.TEACHER.toString())) {
                            role = com.savvas.ltg.rbs.assignments.entity.Role.TEACHER.toString();
                        } else {
                            role = com.savvas.ltg.rbs.assignments.entity.Role.STUDENT.toString();
                        }
                        String searchPrefix = environment.getArgument(ServiceConstants.SEARCH_PREFIX);
                        return assignmentServiceClient.searchAssignments(classId, authScope.getUserId(), role, queryParams, searchPrefix,
                                authContext).flatMap(classAssignmentsSummary -> {
                            classAssignmentsSummary.getClassAssignmentDetailsList().stream().forEach(classAssignmentDetails -> {
                                classAssignmentDetails.setAllStudents(new ArrayList<>(classAssignmentDetails.getAllStudents().stream()
                                        .filter(student -> studentsNeeded.contains(student))
                                        .collect(Collectors.toSet())));
                                classAssignmentDetails.setCompletedStudents(new ArrayList<>(classAssignmentDetails.getCompletedStudents().stream()
                                        .filter(student -> studentsNeeded.contains(student))
                                        .collect(Collectors.toSet())));
                                CompletableFuture<ClassAssignmentDetails> classAssignmentDetailsWithContetItem = getContentItemForClassAssignmentDetails(classAssignmentDetails, classId, authContext, authScope, environment).toFuture();
                            });
                            return Mono.just(classAssignmentsSummary);
                        });
                }).map(result -> result.getT2()).onErrorReturn(new ClassAssignmentsSummary()).toFuture();
    }


    private MultiValueMap<String, String> buildQueryParamsForAssignmentByClassId(DataFetchingEnvironment environment) {
        MultiValueMap<String, String> queryParams = new LinkedMultiValueMap<>();
        try {
            ObjectMapper mapper = new ObjectMapper();
            RequestFilter filter = mapper.convertValue(environment.getArgument(ServiceConstants.QUERY_PARAM_NAME_REQUEST_FILTER), RequestFilter.class);
            if(Objects.nonNull(filter) && MapUtils.isNotEmpty(filter.getFilterMap())) {
                filter.getFilterMap().forEach((key, value) -> {
                    queryParams.add(key, value);
                });
            }
        } catch (Exception excep) {
            LOGGER.error("Exception while parsing request filter data", excep);
        }
        return queryParams;
    }

    private MultiValueMap<String, String> buildQueryParamsRequestFilter(RequestFilter requestFilter) {
        MultiValueMap<String, String> queryParams = new LinkedMultiValueMap<>();
        try {
            if (Objects.nonNull(requestFilter) && MapUtils.isNotEmpty(requestFilter.getFilterMap())) {
                requestFilter.getFilterMap().forEach((key, value) -> {
                    queryParams.add(key, value);
                });
            }
        } catch (Exception excep) {
            LOGGER.error("Exception while parsing request filter data", excep);
        }
        return queryParams;
    }

    private MultiValueMap<String, String> buildQueryParamsForUpdateShowHideFlag(DataFetchingEnvironment environment) {
        MultiValueMap<String, String> queryParams = new LinkedMultiValueMap<>();
        try {
            ObjectMapper mapper = new ObjectMapper();
            RequestFilter filter = mapper.convertValue(environment.getArgument(ServiceConstants.QUERY_PARAM_NAME_REQUEST_FILTER), RequestFilter.class);
            if(Objects.nonNull(filter) && MapUtils.isNotEmpty(filter.getFilterMap())) {
                filter.getFilterMap().forEach((key, value) -> {
                    queryParams.add(key, value);
                });
            }
        } catch (Exception excep) {
            LOGGER.error("Exception while parsing request filter data", excep);
        }
        return queryParams;
    }

    private MultiValueMap<String, String> buildQueryParamsForAssignmentByStudentIdAndClassId(DataFetchingEnvironment environment) {
        MultiValueMap<String, String> queryParams = new LinkedMultiValueMap<>();
        try {
            ObjectMapper mapper = new ObjectMapper();
            RequestFilter filter = mapper.convertValue(environment.getArgument(ServiceConstants.QUERY_PARAM_NAME_REQUEST_FILTER), RequestFilter.class);
            if(Objects.nonNull(filter) && MapUtils.isNotEmpty(filter.getFilterMap())) {
                filter.getFilterMap().forEach((key, value) -> {
                    queryParams.add(key, value);
                });
            }
        } catch (Exception excep) {
            LOGGER.error("Exception while parsing request filter data", excep);
        }
        return queryParams;
    }

    private MultiValueMap<String, String> buildQueryParamsForAssignmentsSummaryByClassId(DataFetchingEnvironment environment) {
        MultiValueMap<String, String> queryParams = new LinkedMultiValueMap<>();
        try {
            ObjectMapper mapper = new ObjectMapper();
            RequestFilter filter = mapper.convertValue(environment.getArgument(ServiceConstants.QUERY_PARAM_NAME_REQUEST_FILTER), RequestFilter.class);
            if(Objects.nonNull(filter) && MapUtils.isNotEmpty(filter.getFilterMap())) {
                filter.getFilterMap().forEach((key, value) -> {
                    queryParams.add(key, value);
                });
            }
            Boolean isListing = Objects.nonNull(environment.getArgument(ServiceConstants.ISLISTING))? environment.getArgument(ServiceConstants.ISLISTING): false;
            queryParams.add("isListing", isListing.toString());
        } catch (Exception exception) {
            LOGGER.error("Exception while parsing request filter data", exception);
        }
        return queryParams;
    }
    private MultiValueMap<String, String> buildQueryParamsForSearchByClassId(DataFetchingEnvironment environment) {
        MultiValueMap<String, String> queryParams = buildQueryParamsForAssignmentsSummaryByClassId(environment);
        queryParams.remove(ServiceConstants.ISLISTING);
        return queryParams;
    }
    public CompletableFuture<ProgramHierarchyAndAssignedToDetails> getProgramHierarchyAndAssignedToDetails(DataFetchingEnvironment environment) throws DataFetchingException {
        String classId = environment.getArgument(ServiceConstants.CLASS_ID);
        String assignmentId = environment.getArgument(ServiceConstants.ASSIGNMENT_ID);
        AssignmentsAuthContext authContext = environment.getContext();
        AuthScope authScope = authContext.getAuthScope();
        String userId = authScope.getUserId();
        assignmentUtils.hasUserAccessOverClass(authScope.getUserId(), classId);
        LOGGER.debug("Getting ProgramHierarachy and assignedTo details for classId {} and assignmentId {}", classId, assignmentId);
        return assignmentServiceClient.getProgramHierarchyAndAssignedToDetails(classId, assignmentId)
                .flatMap(programHierarchyAndAssignedToDetails -> {
                    programHierarchyAndAssignedToDetails = updateGroupnameAndStudentName(programHierarchyAndAssignedToDetails, userId);
                return Mono.just(programHierarchyAndAssignedToDetails);
                }).toFuture();
    }

    public ProgramHierarchyAndAssignedToDetails updateGroupnameAndStudentName(
            ProgramHierarchyAndAssignedToDetails programHierarchyAndAssignedToDetails, String userId) {

        List<AssignedTo> assignedToList = programHierarchyAndAssignedToDetails.getAssignedTo();
        List<AssignedTo> assignedToList1 = new ArrayList();
        List<String> listOfStudents = programHierarchyAndAssignedToDetails.getAssignedTo().stream()
                .map(assignedToObj -> assignedToObj.getStudentUuid()).collect(Collectors.toList());
        userProfileServiceClient.getUserAttributeByUserId(listOfStudents).subscribe(userDetails -> {
            List<RumbaUserAttribute> rumbaUserAttributes = userDetails.getUsers().stream().filter(Objects::nonNull)
                    .filter(user -> Objects.nonNull(user.getRumbaUser())
                            && listOfStudents.contains(user.getRumbaUser().getUserId()))
                    .collect(Collectors.toList());
            if (!assignedToList.isEmpty()) {
                for (AssignedTo assignedTo : assignedToList) {
                    if (StringUtils.isNotEmpty(assignedTo.getStudentUuid())) {
                        String studentFirstName = null;
                        String studentLastName = null;
                        for (RumbaUserAttribute rumbaUserAttribute : rumbaUserAttributes) {
                            if (rumbaUserAttribute.getRumbaUser().getUserId().equals(assignedTo.getStudentUuid())) {
                                studentFirstName = rumbaUserAttribute.getRumbaUser().getFirstName();
                                studentLastName = rumbaUserAttribute.getRumbaUser().getLastName();
                            }
                        }
                        assignedTo.setStudentFirstName(studentFirstName);
                        assignedTo.setStudentLastName(studentLastName);

                    }
                    if (StringUtils.isNotEmpty(assignedTo.getGroupUuid())) {
                        realizeServiceClient.getGroups(userId).subscribe(groups -> {
                            String groupName = null;
                            for (Group group : groups) {
                                if (group.getGroupId().equals(assignedTo.getGroupUuid())) {
                                    groupName = group.getGroupName();
                                }
                            }
                            assignedTo.setGroupName(groupName);
                        });
                    }
                    assignedToList1.add(assignedTo);
                }
                programHierarchyAndAssignedToDetails.setAssignedTo(assignedToList1);
            }
        });
        return programHierarchyAndAssignedToDetails;
    }
    private void updateUserAssignmentLanguage(UserAssignmentLanguage ual, Set<String> processedItems, Set<AssignmentContentItem> assignmentContentItems) {
        if(Objects.nonNull(ual) && !processedItems.contains(ual.getItemUuid())) {
            String id = ual.getItemUuid();
            int version = ual.getItemVersion();
            AssignmentContentItem item = new AssignmentContentItem();
            item.setItemUuid(id);
            item.setItemVersion(version);
            assignmentContentItems.add(item);
            processedItems.add(ual.getItemUuid());
        }
    }
    Mono<AssignmentDetails> getCompositeOtherAssignmentInfo(List<AssignmentDetails> assignmentList, AssignmentDetails otherAssignmentInfo, String classId, String assignmentId, String itemId, CompositeTableOfContentItem compositeTableOfContentItem, DataFetchingEnvironment environment) {

        AssignmentsAuthContext authContext = environment.getContext();
        AuthScope authScope = authContext.getAuthScope();

        if (otherAssignmentInfo.isHasMultipleLanguage()) {
            LOGGER.debug("Got the content details -3 isHasMultipleLanguage for classId {} and assignmentId {}, itemId : {}",
                    classId, assignmentId, itemId);
            Mono<CompositeTableOfContentItem> compositeTableOfContentItemMono = populateOtherLanguagesFromAssignment(compositeTableOfContentItem, otherAssignmentInfo, otherAssignmentInfo.isHasMultipleLanguage(), authContext);
            LOGGER.debug("Got the content details -4 isHasMultipleLanguage for classId {} and assignmentId {}, itemId : {}, compositeTableOfContentItemMono : {}",
                    classId, assignmentId, itemId, compositeTableOfContentItemMono);
            return compositeTableOfContentItemMono.flatMap(contentItem -> {
                LOGGER.debug("Got the content details -5 isHasMultipleLanguage for classId {} and assignmentId {}, itemId : {}, other Lang : {}",
                        classId, assignmentId, itemId, contentItem.getId());
                populateAverageScore(otherAssignmentInfo, authScope, compositeTableOfContentItem, environment);
                assignmentList.add(otherAssignmentInfo);
                return Mono.just(otherAssignmentInfo);
            });
        }
        populateAverageScore(otherAssignmentInfo, authScope, compositeTableOfContentItem, environment);
        assignmentList.add(otherAssignmentInfo);
        return Mono.just(otherAssignmentInfo);
    }

public Mono<ClassAssignmentDetails> getContentItemForClassAssignmentDetails(ClassAssignmentDetails classAssignmentDetails, String classId, AssignmentsAuthContext authContext, AuthScope authScope, DataFetchingEnvironment environment) {
    Boolean isDashBoard = Objects.nonNull(environment.getArgument(ServiceConstants.ISDASHBOARD))? environment.getArgument(ServiceConstants.ISDASHBOARD): false;
    if (requiresPseudoLesson(classAssignmentDetails.getAssignmentType())) {
                LOGGER.debug("Data fetcher for loading the details of assignment for pseudo lesson classId {} and assignmentId {}",
                        classId, classAssignmentDetails.getAssignmentId());
                Mono<CompositeTableOfContentItem> compositeTableOfContentItemMono = getContentItemFromAssignmentForMultiResourcesAssignmentForV3(classAssignmentDetails, authContext);
                return compositeTableOfContentItemMono.flatMap(compositeTableOfContentItem -> {
                    populateAverageScoreForV3(classAssignmentDetails, compositeTableOfContentItem, authScope, environment);
                    return Mono.just(classAssignmentDetails);
                });
            } else {
                Identity identity = getCurrentIdentity(authScope);
                return contentService.getContentItem(classAssignmentDetails.getItemUuid(), String.valueOf(classAssignmentDetails.getItemVersion()),
                        identity, classAssignmentDetails.isHasMultipleLanguage(), authContext).flatMap(item -> {
                    LOGGER.debug("Got the content details -1 for classId {} and assignmentId {}, itemId : {}",
                            classId, classAssignmentDetails.getAssignmentId(), item.getId());
                    CompositeTableOfContentItem compositeTableOfContentItem = new CompositeTableOfContentItem();
                    BeanUtils.copyProperties(item, compositeTableOfContentItem);
                    return removeNewlyAddedContentItemFromAssignmentForV3(classAssignmentDetails, compositeTableOfContentItem, identity,
                            classAssignmentDetails.isAllowMultipleLanguage(), authContext).flatMap(updatedContentItem->  {
                        LOGGER.debug("Got the content details -2 for classId {} and assignmentId {}, itemId : {}",
                                classId, classAssignmentDetails.getAssignmentId(), item.getId());
                        if (classAssignmentDetails.isHasMultipleLanguage()) {
                            LOGGER.debug("Got the content details -3 isHasMultipleLanguage for classId {} and assignmentId {}, itemId : {}",
                                    classId, classAssignmentDetails.getAssignmentId(), item.getId());
                            Mono<CompositeTableOfContentItem> compositeTableOfContentItemMono = populateOtherLanguagesFromAssignmentForV3(compositeTableOfContentItem, classAssignmentDetails, authContext);
                            LOGGER.debug("Got the content details -4 isHasMultipleLanguage for classId {} and assignmentId {}, itemId : {}, compositeTableOfContentItemMono : {}",
                                    classId, classAssignmentDetails.getAssignmentId(), item.getId(), compositeTableOfContentItemMono);
                            return compositeTableOfContentItemMono.flatMap(contentItem -> {
                                LOGGER.debug("Got the content details -5 isHasMultipleLanguage for classId {} and assignmentId {}, itemId : {}, other Lang : {}",
                                        classId, classAssignmentDetails.getAssignmentId(), item.getId(), contentItem.getId());
                                if (isDashBoard) {
                                    classAssignmentDetails.setContentItem(compositeTableOfContentItem);
                                    classAssignmentDetails.getContentItem().setContentItems(Collections.emptyList());
                                } else {
                                    populateAverageScoreForV3(classAssignmentDetails, compositeTableOfContentItem, authScope, environment);
                                }
                                return Mono.just(classAssignmentDetails);
                            });
                        }
                        if (isDashBoard) {
                            classAssignmentDetails.setContentItem(compositeTableOfContentItem);
                            classAssignmentDetails.getContentItem().setContentItems(Collections.emptyList());
                        } else {
                            populateAverageScoreForV3(classAssignmentDetails, compositeTableOfContentItem, authScope, environment);
                        }
                        return Mono.just(classAssignmentDetails);
                    });
                });
            }
        }
    public void populateAverageScoreForV3(ClassAssignmentDetails assignmentDetails, CompositeTableOfContentItem compositeTableOfContentItem, AuthScope authScope, DataFetchingEnvironment environment) {
        assignmentDetails.setContentItem(compositeTableOfContentItem);
        if (isTestNavAssignment(compositeTableOfContentItem)) {
            populateEssayScoringAttributeForV3(assignmentDetails, environment);
        }
        filterAdaptiveAssignments(assignmentDetails, authScope);
        fillAverageScoreForV3(assignmentDetails);
    }
    private void populateEssayScoringAttributeForV3(ClassAssignmentDetails assignment, DataFetchingEnvironment environment) {

        try {
             LOGGER.debug("Calling getAssessmentInfo for assignment {}, environment {}",
                     assignment, environment);
             AssignmentsAuthContext authContext = environment.getContext();
             ServerWebExchange webExchange = authContext.getServerWebExchange();
             AssessmentRequest request = new AssessmentRequest();
             request.setAuthToken(webExchange.getRequest().getHeaders().get(ServiceConstants.AUTHORIZATION).get(0));
             request.setUserId(webExchange.getRequest().getHeaders().get(ServiceConstants.USERID).get(0));
             request.setItemUuid(assignment.getItemUuid());
             request.setItemVersion(assignment.getItemVersion());
             request.setIncludeQuestions(Boolean.TRUE);
             rbsAssessmentServiceClient.getAssessmentInfo(request).map(assessmentInfo -> {
                 assignment.setHasEssayScoring(assessmentInfo.isHasEssayScoring());
                 if (assessmentInfo.isHasEssayScoring()) {
                     assignment.setMaxScore(assessmentInfo.getMaxScore());
                 }
                 return assignment;
             }).subscribe();
         } catch (Exception excep) {
             LOGGER.error("Exception while getting Assessment Info details", excep);
         }
     }
    void fillAverageScoreForV3(ClassAssignmentDetails assignment) {
        if (assignment.getContentItem().isLesson()) {
            populateLessonAssignmentInfoForV3(assignment);
        }
        List<Double> scores = assignmentUtils.getScoresForV3(assignment);
        boolean includeNulls = false;
        assignment.setAverageScore(assignmentUtils.calculateAverage(scores, includeNulls));
    }
    void populateLessonAssignmentInfoForV3(ClassAssignmentDetails assignment) {
        List<UserAssignment> userAssignments = assignment.getStudentMetadata();
        populateLessonAssignmentInfo(userAssignments, assignment.getAssignmentId());
    }
    Mono<CompositeTableOfContentItem> getContentItemFromAssignmentForMultiResourcesAssignmentForV3(ClassAssignmentDetails classAssignmentDetails, AssignmentsAuthContext authContext) {
        LOGGER.debug("Inside getContentItemFromAssignmentForMultiResourcesAssignment assignmentId {}",
            classAssignmentDetails.getAssignmentId());
        boolean getMultiLanguagesWithSearch = false;
        Collection<AssignmentContentItem> itemInfo = toCreateContentItemsForMultiResourceForV3(classAssignmentDetails, authContext.getAuthScope());
        Mono<CompositeTableOfContentItem> compositeTableOfContentItemLessonMono = getContentItemForMultiResourcesAssignment(classAssignmentDetails.getItemUuid(), itemInfo, getMultiLanguagesWithSearch, authContext);
        if (classAssignmentDetails.isHasMultipleLanguage()) {
            return compositeTableOfContentItemLessonMono.flatMap(contentItem -> {
                return populateOtherLanguagesFromAssignmentForV3(contentItem, classAssignmentDetails, authContext);
            });
        }
        return compositeTableOfContentItemLessonMono;
    }
    private Collection<AssignmentContentItem> toCreateContentItemsForMultiResourceForV3(ClassAssignmentDetails classAssignmentDetails, AuthScope authScope) {
        LOGGER.debug("Inside  toCreateContentItemsForMultiResource assignmentId {}",
           classAssignmentDetails.getAssignmentId());
        Predicate<UserAssignmentLanguage> studentFilter = userAssignmentLanguage -> userAssignmentLanguage.getIsSelected();
        Predicate<UserAssignmentLanguage> teacherFilter = userAssignmentLanguage -> userAssignmentLanguage.getIsdefault();
        Predicate<UserAssignmentLanguage> userAssignmentLanguageFilter = hasRole(authScope, Role.STUDENT.toString()) ? studentFilter : teacherFilter;
        Set<AssignmentContentItem> assignmentContentItems = new LinkedHashSet<>();
        Set<String> processedItems = new HashSet<>();
        classAssignmentDetails.getStudentMetadata().forEach(userAssignment -> {
            if (!userAssignment.isItemTypeRRSSCO() && CollectionUtils.isNotEmpty(userAssignment.getUserAssignmentLanguageList())) {
                UserAssignmentLanguage ual = userAssignment.getUserAssignmentLanguageList()
                        .stream().filter(userAssignmentLanguageFilter).findFirst().orElse(null);
                if (Objects.isNull(ual)) {
                    if (!StringUtils.equals(classAssignmentDetails.getAssignmentId(), userAssignment.getAssignmentId())
                            && (userAssignment.isItemTypeSequence() || userAssignment.isItemTypeLesson() ||
                            userAssignment.isItemTypeMultistage())) {
                        ual = userAssignment.getUserAssignmentLanguageList().get(0);
                    }
                }
                if (Objects.nonNull(ual) && !processedItems.contains(ual.getItemUuid())) {
                    String id = ual.getItemUuid();
                    int version = ual.getItemVersion();
                    AssignmentContentItem item = new AssignmentContentItem();
                    item.setItemUuid(id);
                    item.setItemVersion(version);
                    assignmentContentItems.add(item);
                    processedItems.add(ual.getItemUuid());
                }
            }
        });
        assignmentContentItems.removeIf(filter -> filter.getItemUuid().equalsIgnoreCase(classAssignmentDetails.getItemUuid()));
        return assignmentContentItems;
    }
    /**
     * Extract contentItem ids & versions from userAssignments in userAssignmentList (i.e., from userAssignment.userAssignmentLanguageList),
     * retrieve corresponding ContentItems, and store as "map of lists" where the map key is contentItem primaryLanguageItemId.
     *
     * @param assignment
     * @return
     */
    Mono<CompositeTableOfContentItem> populateOtherLanguagesFromAssignmentForV3(CompositeTableOfContentItem contentItem, ClassAssignmentDetails classAssignmentDetails,
                                                                           AssignmentsAuthContext authContext) {
        LOGGER.debug("Inside populateOtherLanguagesFromAssignment contentItem : {}, assignmentId {}, otherLanguage : {}",
                contentItem, classAssignmentDetails.getAssignmentId(), classAssignmentDetails.isHasMultipleLanguage());

        Map<String, UserAssignmentLanguage> itemIds = new HashMap<>();
        classAssignmentDetails.getStudentMetadata().stream().filter(studentMetadata -> !studentMetadata.isItemTypeRRSSCO())
                .forEach(userAssignment -> {
                    userAssignment.getUserAssignmentLanguageList().forEach(userAssignmentLanguage -> {
                        if (!itemIds.containsKey(userAssignmentLanguage.getItemUuid()) && !StringUtils
                                .equalsAnyIgnoreCase(classAssignmentDetails.getItemUuid(), userAssignmentLanguage.getItemUuid())) {
                            itemIds.put(userAssignmentLanguage.getItemUuid(), userAssignmentLanguage);
                        }
                    });
                });

        Map<String, List<TableOfContentItem>> allContentItems = new HashMap<>();

        Mono<List<TableOfContentItem>> listMono = Flux.fromIterable(itemIds.values())
                .flatMap(userAssignmentLanguage -> contentService.getContentItem(userAssignmentLanguage.getItemUuid(),
                        String.valueOf(userAssignmentLanguage.getItemVersion()), getCurrentIdentity(authContext.getAuthScope()),
                        classAssignmentDetails.isHasMultipleLanguage(), authContext).flatMap(item -> Mono.just(item)))
                .collectList();

        return listMono.flatMap(tableOfContentItems -> {
            if (CollectionUtils.isNotEmpty(tableOfContentItems)) {
                tableOfContentItems.stream().filter(Objects::nonNull).forEach(tableOfContentItem -> {
                    populateContentItem(tableOfContentItem.getPrimaryLanguageItemId(), allContentItems,
                            tableOfContentItem);
                });
            }
            return populateOtherLanguages(contentItem, allContentItems).flatMap(compositeContentItem -> Mono.just(compositeContentItem));
        });
    }

    private Boolean isAdaptiveAssignmentForV3(ClassAssignmentDetails assignmentDetails){
        return assignmentDetails.getAssignmentType().equals(AssignmentType.ADAPTIVE);
    }
    private void adaptiveAssignmentsSummaryTeacherViewForV3(ClassAssignmentDetails assignmentDetails, AuthScope authScope) {
        if (Boolean.TRUE.equals(isAdaptiveAssignmentForV3(assignmentDetails))
                && hasRole(authScope, Role.TEACHER.toString())) {
            List<UserAssignment> summarizedStudentMetaData =
                    summaryAdaptiveTaskAssignmentForV3(assignmentDetails.getStudentMetadata());
            assignmentDetails.setStudentMetadata(summarizedStudentMetaData);
        }
    }

    private Mono<CompositeTableOfContentItem> removeNewlyAddedContentItemFromAssignmentForV3(ClassAssignmentDetails assignment, CompositeTableOfContentItem assignmentItem,
            Identity identity, boolean multiLangAssignment, AssignmentsAuthContext authContext) {
        LOGGER.debug("Inside removeNewlyAddedContenItemFromAssignment assignmentId {}", assignment.getAssignmentId());
        Set<AssignmentContentItem> assignmentContentItems = new HashSet<>();
        if (assignmentItem.isSequence() || assignmentItem.isLesson()) {
            //Creating an object of all the item_uuids for all content items for which assignment was created
            for (UserAssignment userAssignment : assignment.getStudentMetadata()) {
                //excluding RRSCO type of item ids
                if (!userAssignment.isItemTypeRRSSCO()) {
                    assignmentContentItems.addAll(userAssignment.getUserAssignmentLanguageList().stream().filter(
                            userAssignmentLang -> userAssignmentLang.getIsSelected()).map(mapAssignmentContentItem).collect(Collectors.toList()));
                }
            }
            if (CollectionUtils.isNotEmpty(assignmentContentItems)) {
                String id = assignmentItem.getId();
                assignmentContentItems.removeIf(item -> StringUtils.equals(item.getItemUuid(), id));
                // filtering the child content  items based on the assignment item-uuids
                Set<String> assignmentIdsWhenAssigned = assignmentContentItems.stream().map(AssignmentContentItem::getItemUuid).collect(Collectors.toSet());
                Set<String> currentItemIds = getCurrentLeafAssignmentItems(assignmentItem);
                if (CollectionUtils.isNotEmpty(removeCommonItemIdsAddDifference(assignmentIdsWhenAssigned, currentItemIds, multiLangAssignment))) {
                    //If there are items added or removed after the assignment has been created we need to filter/add the contentItems
                    return addRemoveContentItemsInLesson(assignmentContentItems, assignmentItem, currentItemIds, identity, multiLangAssignment, authContext);
                }
            }
        }
        return Mono.just(assignmentItem);
    }

    public CompletableFuture<Map<String, StudentClassAssignment>> getStudentAssignmentSummaries(DataFetchingEnvironment environment) throws DataFetchingException {
        String classId = environment.getArgument(ServiceConstants.CLASS_ID);
        String teacherRole = com.savvas.ltg.rbs.assignments.entity.Role.TEACHER.toString();
        AssignmentsAuthContext authContext = environment.getContext();
        AuthScope authScope = authContext.getAuthScope();
        String userId = authScope.getUserId();
        assignmentUtils.hasUserAccessOverClass(authScope.getUserId(), classId);
        int allAssignments = -1;
        boolean includeRemedialAssignments = true;
        RequestFilter filter = new RequestFilter();
        filter.setPageSize(allAssignments);
        filter.setIncludeRemedialAssignments(includeRemedialAssignments);

        return rosterService.getSectionDetails(classId, authScope.getUserId()).map(classRosterDetails -> classRosterDetails)
                .zipWhen((classRosterDetails) -> {
                    Map<String, StudentClassAssignment> studentToAssignmentMappings = new HashMap<String, StudentClassAssignment>();
                    List<String> listOfStudents = classRosterDetails.getCmsClass().getData().getSection().getData().getSectionInfo().getStudents().stream()
                                            .map(studentObj -> studentObj.getStudentPiId()).collect(Collectors.toList());
                    for (String student: listOfStudents) {
                        studentToAssignmentMappings.put(student, new StudentClassAssignment(new ArrayList<UserAssignment>()));
                    }
                    MultiValueMap<String, String> queryParams = buildQueryParamsRequestFilter(filter);
                        return  assignmentServiceClient.getAssignmentsByClassId(classId, userId,
                                teacherRole, queryParams, authContext).flatMap(classAssignment -> {
                                    classAssignment.getAssignments().stream().forEach(assignment -> {
                                        if (isAdaptiveAssignment(assignment) || isMultistageAssignment(assignment)){
                                            assignment.setStudentMetadata(filterSequenceUserAssignment(assignment.getStudentMetadata()));
                                        }
                                        if (assignment.containsLesson()) {
                                            populateLessonAssignmentInfo(assignment);
                                        }
                                        addUserAssignmentsToStudentSummaries(studentToAssignmentMappings, assignment);
                                    });
                            return Mono.just(studentToAssignmentMappings);
                        });
                }).map(result -> result.getT2()).onErrorReturn(new HashMap<String, StudentClassAssignment>()).toFuture();
    }

    private List<UserAssignment> filterSequenceUserAssignment(List<UserAssignment> studentMetadata) {
        List<UserAssignment> sequenceUserAssignment = new ArrayList<>();
        for (UserAssignment userAssignment: studentMetadata){
            if (userAssignment.isItemTypeSequence() || userAssignment.isItemTypeMultistage()){
                if(null != userAssignment.getLatestUserAssignmentData()) {
                    userAssignment.setUserAssignmentDataList(new ArrayList<UserAssignmentData>());
                }
                sequenceUserAssignment.add(userAssignment);
            }
        }
        return sequenceUserAssignment;
    }

    private void addUserAssignmentsToStudentSummaries(
            Map<String, StudentClassAssignment> studentToAssignmentMappings, AssignmentDetails assignment) {
        for (UserAssignment userAssignment : assignment.getStudentMetadata()) {
            for (UserAssignmentLanguage userAssignmentLanguage : userAssignment.getUserAssignmentLanguageList()) {
                if (StringUtils.equals(assignment.getItemUuid(), userAssignmentLanguage.getItemUuid())) {
                    addUserAssignmentToStudentSummary(studentToAssignmentMappings, userAssignment);
                }
            }
        }
    }

    private void addUserAssignmentToStudentSummary(Map<String, StudentClassAssignment> studentToAssignmentMappings, UserAssignment userAssignment) {
        StudentClassAssignment studentSummary = studentToAssignmentMappings.get(userAssignment.getStudentUuid());
        if (studentSummary != null) {
            List<UserAssignment> studentAssignments = studentSummary.getUserAssignments();
            studentAssignments.add(userAssignment);
        }
    }
    
    private List<UserAssignment> summaryAdaptiveTaskAssignmentForV3(List<UserAssignment> studentMetaData) {
        Collections.sort(studentMetaData, new UserAssignmentComparator());
        List<UserAssignment> summarizedStudentMetaData = new ArrayList<>();
        Map<String, UserAssignment> userAssignmentMap = new HashMap<>();
        for (UserAssignment userAssignment:studentMetaData) {
            if (userAssignment.isItemTypeSequence()) {
                summarizedStudentMetaData.add(userAssignment);
                userAssignmentMap.put(userAssignment.getStudentUuid(), userAssignment);
            }
        }
        return summarizedStudentMetaData;
    }
    
    private void filterAdaptiveAssignments(ClassAssignmentDetails classAssignmentDetails, AuthScope authScope) {
        if (isAdaptiveAssignmentForV3(classAssignmentDetails) && hasRole(authScope, Role.TEACHER.toString())) {
            List<String> completedStudentIds = new ArrayList<String>();
            List<UserAssignment> summarizedStudentMetaData = summaryAdaptiveTaskAssignmentForV3(classAssignmentDetails.getStudentMetadata());
            classAssignmentDetails.setStudentMetadata(summarizedStudentMetaData);
            for (UserAssignment userAssignment: summarizedStudentMetaData) {
                if (userAssignment != null && userAssignment.isMarkCompleted() && !completedStudentIds.contains(userAssignment.getStudentUuid())) {
                    completedStudentIds.add(userAssignment.getStudentUuid());
                }
            }
            classAssignmentDetails.setCompletedStudents(completedStudentIds);
        }
        adaptiveAssignmentsSummaryTeacherViewForV3(classAssignmentDetails, authScope);
    }
}
